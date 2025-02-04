from datetime import date
from enum import Enum

from hgraph import TSD, TS, subscription_service, TSS, TSL, Size, operator, SIZE, graph, map_, \
    convert, switch_, if_then_else, sum_, merge, unpartition, DEFAULT, clone_type_var, \
    TIME_SERIES_TYPE, mul_, pass_through

from hg_systematic.operators._calendar import calendar_for, HolidayCalendar

__all__ = ["INDEX_WEIGHTS", "index_composition", "index_assets", "index_rolling_contracts", "index_rolling_weights",
           "index_weights", "DEFAULT_INDEX_PATH", "ContractMatchState", "index_rolling_schedule"]

INDEX_WEIGHTS = TSD[str, TS[float]]

DEFAULT_INDEX_PATH = "index_default"


# Describe the composition of an index

@subscription_service
def index_composition(symbol: TS[str], path: str = DEFAULT_INDEX_PATH) -> INDEX_WEIGHTS:
    """
    The weights for the given index as identified by the symbol.
    """


@subscription_service
def index_assets(symbol: TS[str], path: str = DEFAULT_INDEX_PATH) -> TSS[str]:
    """
    The set of assets defining the index. This is point-in-time.
    """


ROLLING_SCHEDULE = clone_type_var(TIME_SERIES_TYPE, "ROLLING_SCHEDULE")


@subscription_service
def index_rolling_schedule(symbol: TS[str],
                           tp: type[ROLLING_SCHEDULE] = TSD[str, TSD[int, TSL[TS[int], Size[2]]]],
                           path: str = DEFAULT_INDEX_PATH) -> DEFAULT[ROLLING_SCHEDULE]:
    """
    A rolling schedule for the index. The default shape of the schedule is the
    assert symbol, followed by a dictionary keyed by month and then a tuple of
    month and year offset.
    symbol -> month (now) -> (month (contract), year offset)

    This is suitable for monthly or less frequent rolling contracts, where the
    interpretation would be that given the current month, what is the contract
    that should be held at the start of the month.
    """


@operator
def index_rolling_contracts(rule: str, symbol: TS[str], dt: TS[date], sz: type[DEFAULT[SIZE]] = Size[2]) -> TSL[
    TS[str], SIZE]:
    """
    The current rolling contracts for given asset for the given index.
    This lines up with the index_roll_weights.

    The asset represents the base contract spec, the date orients the roll in time.
    For example:

        (ZCZ23, ZCH24)

    Represents the corn future and the rolling pair for November 2023 in the BCOM index.
    The next month the pair would be:

        (ZCH24, ZCH24)

    (i.e. we are not rolling this month)

    The combination of the rolling rule, which allocates the weights to each pair and the actual
    values of the pairs allows for simple rolling to be implemented.
    """


@operator
def index_rolling_weights(
        rule: str,
        calendar: HolidayCalendar,
        dt: TS[date],
        sz: SIZE = Size[2]
) -> TSL[TS[float], SIZE]:
    """
    The rule attribute is used to identify the rolling rule. The use would look like:

    index_rolling_weights("BCOM", calendar_for("ZC"), dt)

    This produces a time-series of weights describing the rate of switch from one contract to another.

    Where the contract type would be a time-series.
    """


@graph
def index_weights(symbol: TS[str], dt: TS[date], rolling_rule: str,
                  rolling_size: type[SIZE] = Size[2]) -> INDEX_WEIGHTS:
    assets = index_assets(symbol)
    asset_weights = index_composition(symbol)
    calendar = calendar_for(symbol)
    rolling_contracts = map_(
        lambda key, dt_: index_rolling_contracts(rolling_rule, key, dt_, sz=rolling_size), dt, __keys__=assets)
    rolling_weights = map_(
        lambda key, dt_, calendar_: index_rolling_weights(rolling_rule, calendar_, dt_, sz=rolling_size), dt,
        pass_through(calendar), __keys__=assets)
    index_weights = map_(lambda rw, aw: mul_(rw, aw), rolling_weights, asset_weights)
    tsd_index_weights = map_(lambda rc, cw: _to_tsd(rc, cw), rolling_contracts, index_weights)
    # The above is a map: asset->contract->weight, we want this to be a map: contract->weight, use unpartition.
    weights = unpartition(tsd_index_weights)
    return weights


class ContractMatchState(Enum):
    ALL_KEYS_MATCH = 1
    LEFT_ONLY = 2
    RIGHT_ONLY = 3
    OTHERWISE = 4


@graph(requires=lambda m, s: m[SIZE].py_type == Size[2])
def _to_tsd(keys: TSL[TS[str], SIZE], values: TSL[TS[float], SIZE]) -> TSD[str, TS[float]]:
    """
    For now, we only consider processing a tuple with 2 keys and values.
    This logic could be generalised, but for now we only need to deal with this scenario.
    """
    match = keys[0] == keys[1]
    lhs = values[1] == 0.0
    rhs = values[0] == 0.0
    match_case = if_then_else(match, ContractMatchState.ALL_KEYS_MATCH,
                              if_then_else(lhs, ContractMatchState.LEFT_ONLY,
                                           if_then_else(rhs, ContractMatchState.RIGHT_ONLY, ContractMatchState.OTHERWISE)))
    out = switch_(
        {
            ContractMatchState.ALL_KEYS_MATCH: lambda k, v: convert[TSD](k[0], sum_(v, 0.0)),
            ContractMatchState.LEFT_ONLY: lambda k, v: convert[TSD](k[0], v[0]),
            ContractMatchState.RIGHT_ONLY: lambda k, v: convert[TSD](k[1], v[1]),
            ContractMatchState.OTHERWISE: lambda k, v: merge(convert[TSD](k[0], v[0]), convert[TSD](k[1], v[1]),
                                                             disjoint=True),
        },
        match_case,
        keys,
        values
    )
    return out


