from datetime import date
from typing import Callable, Generic

from hg_oap.instruments.future import month_code
from hgraph import TSD, TS, Size, operator, SIZE, graph, map_, \
    if_then_else, pass_through, TimeSeriesSchema, TSB, SCALAR, reduce, div_, \
    DivideByZero, passive, union, flip, feedback, ts_schema, sample, lag, default, and_, dedup, component, explode, \
    format_, lift

from hg_systematic.operators._calendar import business_day, calendar_for, HolidayCalendar, filter_by_calendar
from hg_systematic.operators._price import price_in_dollars

__all__ = [
    "INDEX_ROLL_FLOAT", "INDEX_ROLL_STR", "index_composition", "index_rolling_contracts",
    "index_rolling_weight", "symbol_is", "index_level", "roll_contracts_monthly"
]

"""
An attempt to describe index computations in a mostly generic way, for now we will constraint
the solution to indices that roll from one underlyer to another (i.e. only two active underlyers for an asset
at any one time)
"""


class _IndexRollingOutput(TimeSeriesSchema, Generic[SCALAR]):
    """
    An output structure representing a floating point values for an operator. The keys are the assets making up the
    index, the values represent the first and second futures involved in the roll. The values are aligned to the
    contracts defined in ``index_rolling_contracts``.
    """
    first: TSD[str, TS[SCALAR]]
    second: TSD[str, TS[SCALAR]]


INDEX_ROLL_FLOAT = TSB[_IndexRollingOutput[float]]
INDEX_ROLL_STR = TSB[_IndexRollingOutput[str]]


def symbol_is(symbol: str, sz: SIZE = Size[2]) -> Callable[[dict, dict], bool]:
    """
    Used to match the ``requires`` rule in the ``overloads`` implementation
    For example:

    ::

        @graph(overloads=index_composition, requires=symbol_is("BCOM Index"))
        def bcom_index_composition(symbol: str, dt: TS[date]) -> INDEX_ROLLING_OUTPUT:
            ...
    """
    return lambda m, s: s["symbol"] == symbol


@operator
def index_composition(symbol: str, dt: TS[date], calendar: HolidayCalendar) -> INDEX_ROLL_FLOAT:
    """
    The index composition for the given date for the given symbol. This composition is used
    as the weighting to compute the level of the assets from the given contracts and their prices.
    """


@operator
def index_rolling_weight(symbol: str, dt: TS[date], calendar: HolidayCalendar) -> TS[float]:
    """
    For a date what is the rolling weight between first and second contracts.
    This is the rolling weight and not the asset weights. With 1.0 representing all
    the weight on the first contract and 0.0 representing all the weight on the second.
    This must align with the ``index_rolling_contracts`` determination of first and second contracts.

    It is useful to build a rolling weight using the ``if_cmp`` operator.
    Consider:

    ::

        is_rolling: TS[CmpResult]
        return if_cmp(is_rolling, 1.0, roll_value, 0.0)

    Alternatively, this can be achieved using a switch:

    ::

        is_rolling: TS[CmpResult]
        return switch_({
            CmpResult.LT: lambda ...: 1.0,
            CmpResult.EQ: lambda ...: ...,  # Compute rolling weight
            CmpResult.GT: lambda ...: 0.0,
        }, is_rolling, ...)
    """


@operator
def index_rolling_contracts(symbol: str, dt: TS[date], calendar: HolidayCalendar) -> INDEX_ROLL_STR:
    """
    The first and second contracts for this rolling period.
    For example, for a monthly rolling period, the first and second contracts could be:

    ::
        {
            'ZC': {
                'first': 'ZCZ23',
                'second': 'ZCH24',
            },
            ...
        }

    Then, if there is no roll in the rolling period, it may take the form of:

    ::

        {
            'ZC': {
                'first': 'ZCH24',
                'second': 'ZCH24',
            },
            ...
        }

    This would indicate a non-roll in this rolling period.

    NOTE: The alignment of all rolling operators for a given instrument must be observed to get correct behaviour.
    """


@graph
def weighted_average_value(
        weights: TSD[str, TS[float]],
        contracts: TSD[str, TS[str]],
        prices: TSD[str, TS[float]]
) -> TS[float]:
    """
    Compute the weighted average value for the given weights, contracts and prices.
    """
    values = map_(lambda w, c, p: w * p[c], weights, contracts, pass_through(prices))
    return reduce(lambda x, y: x + y, values, 0.0)


_ComputeIndexLevelsOther = ts_schema(
    new_period=TS[bool],
    rolling_weight=TS[float],
    level_prev=TS[float],
    wav_first_prev=TS[float],
    wav_second_prev=TS[float],
)


def compute_index_levels(
        weights: INDEX_ROLL_FLOAT,
        contracts: INDEX_ROLL_STR,
        prices: TSD[str, TS[float]],
        other: TSB[_ComputeIndexLevelsOther],
        rounding_fn: Callable[[TS[float]], TS[float]] = None
) -> TSB[ts_schema(level=TS[float], wav_first=TS[float], wav_second=TS[float])]:
    # NOTE: We bundle up the single value ticks into a tsb to make recording
    # more simplistic as it will group up the results into a single dataframe.
    new_period = other.new_period
    rolling_weight = other.rolling_weight
    level_prev = other.level_prev
    wav_first_prev = other.wav_first_prev
    wav_second_prev = other.wav_second_prev
    wav_first = weighted_average_value(weights.first, contracts.first, prices)
    wav_second = weighted_average_value(weights.second, contracts.second, prices)
    first_rw = rolling_weight
    second_rw = 1.0 - rolling_weight

    # If we are in the first day of the new period, the first previous is actually
    # The second previous since we now consider the structure from second to
    # now be the structure of first.
    wav_first_prev = if_then_else(new_period, wav_second_prev, wav_first_prev)

    # We mark the previous value contributions as passive to ensure that the feedback
    # of these values does not compute a new value
    value = passive(level_prev) * div_(
        wav_first * first_rw + wav_second * second_rw,
        passive(wav_first_prev * first_rw + wav_second_prev * second_rw),
        DivideByZero.ONE
    )
    if rounding_fn:
        value = rounding_fn(value)
    return value


@graph
def index_level(symbol: str, initial_level: float = 100.0, record: str = None,
                rounding_fn: Callable[[TS[float]], TS[float]] = None) -> TS[float]:
    dt = business_day(symbol)
    calendar = calendar_for(symbol)

    weights = index_composition(symbol, dt, calendar)
    rolling_weight = index_rolling_weight(symbol, dt, calendar)
    contracts = index_rolling_contracts(symbol, dt, calendar)

    all_contracts = union(flip(contracts.first).key_set, flip(contracts.second).key_set)
    prices = map_(lambda key, d, c: filter_by_calendar(price_in_dollars(key)), __keys__=all_contracts, d=dt, c=calendar)

    level_fb = feedback(TS[float], initial_level)
    wav_first_fb = feedback(TS[float], 0.0)
    wav_second_fb = feedback(TS[float], 0.0)

    # We have a new period if the weight was 0.0 and is now 1.0
    # We default to the previous state being 0.0, this should not make
    # a difference as we initialise the previous wavs with the same value
    new_period = dedup(and_(
        rolling_weight == 1.0,
        default(lag(rolling_weight), 0.0) == 0.0
    ))

    # We don't wrap the function immediately and then delay until we know
    # we want to record or not, if we do we wrap with component and set
    # the recorded id to be the record string value. Allowing for named
    # Recording if we want to support multiple instance of index level
    # computation and record the results independently.
    if record:
        fn = component(compute_index_levels, recordable_id=record)
    else:
        fn = graph(compute_index_levels)

    level_output = fn(
        weights, contracts, prices,
        TSB[_ComputeIndexLevelsOther].from_ts(
            new_period=new_period, rolling_weight=rolling_weight, level_prev=level_fb(),
            wav_first_prev=wav_first_fb(), wav_second_prev=wav_second_fb()
        ),
        rounding_fn=rounding_fn
    )

    # We want to ensure we only capture a level value when we are expecting a value.
    # This ensures we don't accidentally compute a level we are not expecting.
    # If we want indicative levels we could return a live copy of this as well, but
    # the previous level should always be the official previous level.
    level = sample(dt, level_output)

    level_fb(level)
    wav_first_fb(level_output.wav_first)
    wav_second_fb(level_output.wav_second)

    return level


@graph
def roll_contracts_monthly(
        dt: TS[date],
        roll_schedule: TSD[str, TSD[int, TS[tuple[int, int]]]],
        format_str: TSD[str, TS[str]],
        year_scale: TSD[str, TS[int]]
) -> INDEX_ROLL_STR:
    """
    From the given date, will look up this month and next months' contracts.
    The assumption is that by supplying a month letter and a year value (int)
    (as scaled by the year_scale value) to the format_str, it is possible to compute the contract
    name.
    """
    y1, m1, _ = explode(dt)
    m2 = (m1 % 12) + 1
    ro = m2 < m1
    y2 = if_then_else(ro, y1+1, y1)

    c1_m = map_(
        _create_contract,
        month=m1,
        year=y1,
        schedule=roll_schedule,
        format_str=format_str,
        year_scale=year_scale
    )

    c2_m = map_(
        _create_contract,
        month=m2,
        year=y2,
        schedule=roll_schedule,
        format_str=format_str,
        year_scale=year_scale
    )

    return INDEX_ROLL_STR.from_ts(first=c1_m, second=c2_m)


@graph
def _create_contract(month: TS[int], year: TS[int], schedule: TSD[int, TS[tuple[int, int]]], format_str: TS[str], year_scale: TS[int]) -> TS[str]:
    s = schedule[month]
    m = s[0]
    y = (year + s[1]) % year_scale
    return format_(
        format_str,
        month=lift(month_code, inputs={"d": TS[int]})(m),
        year=y
    )
