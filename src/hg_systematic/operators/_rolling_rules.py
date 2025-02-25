from dataclasses import dataclass
from datetime import date

from hg_oap.instruments.future import month_code
from hgraph import TimeSeriesSchema, TS, subscription_service, default_path, CompoundScalar, graph, TSD, explode, \
    if_then_else, map_, lift, format_, switch_, TSB, dedup, debug_print, TSL, Size

from hg_systematic.operators import INDEX_ROLL_STR

__all__ = ["MonthlyRollingRange", "monthly_rolling_weights", "MonthlyRollingWeightRequest", "roll_contracts_monthly",
           "rolling_contracts_for"]

from hg_systematic.operators._calendar import next_month


@dataclass(frozen=True)
class MonthlyRollingWeightRequest(CompoundScalar):
    """
    Specified a linear roll over the range specified.
    The start can be negatively offset to indicate the roll to this month's contract
    starts in the prior month.
    The start and end date may never overlap, there MUST be the opportunity for at
    least one value of 1.0 and one of 0.0 in any month.
    """
    start: int
    end: int
    calendar_name: str
    round_to: int


@subscription_service
def monthly_rolling_weights(request: TS[MonthlyRollingWeightRequest], path: str = default_path) -> TS[float]:
    """
    Produces a stream of rolling weights over the given calendars business days.
    This will only tick a value if the result is modified, i.e. it does not tick
    each time a date changes, but only when the result is different.
    """


@dataclass
class MonthlyRollingRange(TimeSeriesSchema):
    start: TS[int]
    end: TS[int]
    first_day: TS[int]  # This is the same as start when start is a positive value, and is the day index of the
    # previous month when negative


@graph
def roll_contracts_monthly(
        dt: TS[date],
        roll_schedule: TSD[str, TSD[int, TS[tuple[int, int]]]],
        format_str: TSD[str, TS[str]],
        year_scale: TSD[str, TS[int]],
        day_index: TS[int] = None,
        roll_range: TSB[MonthlyRollingRange] = None
) -> INDEX_ROLL_STR:
    """
    From the given date, will look up this month and next months' contracts.
    The assumption is that by supplying a month letter and a year value (int)
    (as scaled by the year_scale value) to the format_str, it is possible to compute the contract
    name.
    """
    if roll_range is not None:
        assert day_index is not None, "day_index is required to be set when roll_range is set."
        dt = switch_(
            roll_range.start < 0,
            {
                True: lambda d, di, rr: switch_(
                    di > rr.end,
                    {
                        True: lambda d_: next_month(d_),
                        False: lambda d_: d_
                    },
                    d
                ),
                False: lambda d, di, rr: d
            },
            dt,
            day_index,
            roll_range
        )
    debug_print("dt", dt)
    return _roll_contracts_monthly(dt, roll_schedule, format_str, year_scale)


@graph
def _roll_contracts_monthly(
        dt: TS[date],
        roll_schedule: TSD[str, TSD[int, TS[tuple[int, int]]]],
        format_str: TSD[str, TS[str]],
        year_scale: TSD[str, TS[int]],
) -> INDEX_ROLL_STR:
    """Perform the rolling logic"""
    y1, m1, _ = explode(dt)
    m2 = (m1 % 12) + 1
    ro = m2 < m1
    y2 = if_then_else(ro, y1 + 1, y1)

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
def _create_contract(month: TS[int], year: TS[int], schedule: TSD[int, TS[tuple[int, int]]], format_str: TS[str],
                     year_scale: TS[int]) -> TS[str]:
    s = schedule[month]
    m = s[0]
    y = (year + s[1]) % year_scale
    return format_(
        format_str,
        month=lift(month_code, inputs={"d": TS[int]})(m),
        year=y
    )


@subscription_service
def rolling_contracts_for(symbol: TS[str], path: str = default_path) -> TSL[TS[str], Size[2]]:
    """
    The rolling contracts for the symbol.
    """
