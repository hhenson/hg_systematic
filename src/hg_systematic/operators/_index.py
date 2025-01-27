from collections.abc import Callable
from datetime import date
from enum import Enum

from hgraph import TSD, TS, subscription_service, default_path, TSS, TSL, Size, TSB, operator, SIZE, graph, map_, \
    combine, convert, switch_, if_then_else, sum_, merge, unpartition
from hg_systematic.operators._calendar import HolidayCalendarSchema, calendar_for

__all__ = ["INDEX_WEIGHTS", "index_composition", "index_assets", "index_rolling_contracts", "index_rolling_weights",
           "index_weights"]

INDEX_WEIGHTS = TSD[str, TS[float]]


# Describe the composition of an index

@subscription_service
def index_composition(symbol: TS[str], path: str = default_path) -> INDEX_WEIGHTS:
    """
    The weights for the given index as identified by the symbol.
    """


@subscription_service
def index_assets(symbol: TS[str], path: str = default_path) -> TSS[str]:
    """
    The set of assets defining the index. This is point-in-time.
    """


@subscription_service
def index_rolling_contracts(symbol: TS[str], path: str = default_path) -> TSD[str, TSL[TS[str], Size[2]]]:
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
        calendar: TSB[HolidayCalendarSchema],
        dt: TS[date],
        sz: SIZE = Size[2]
) -> TSL[TS[int], SIZE]:
    """
    The rule attribute is used to identify the rolling rule. The use would look like:

    index_rolling_weights("BCOM", calendar_for("ZC"), dt)

    This produces a time-series of weights describing the rate of switch from one contract to another.

    Where the contract type would be a time-series.
    """


@graph
def index_weights(symbol: TS[str], dt: TS[date], rolling_rule: str) -> INDEX_WEIGHTS:
    assets = index_assets(symbol)
    asset_weights = index_composition(symbol)
    rolling_contracts = map_(lambda x: index_rolling_contracts(x), assets)
    rolling_weights = map_(lambda x, dt_: index_rolling_weights(rolling_rule, calendar_for(x), dt_), assets, dt)
    index_weights = map_(lambda rw, aw: combine[TSL](rw * aw), rolling_weights, asset_weights)
    tsd_index_weights = map_(lambda rc, cw: _to_tsd(rc, cw), rolling_contracts, index_weights)
    # The above is a map: asset->contract->weight, we want this to be a map: contract->weight, use unpartition.
    return unpartition(tsd_index_weights)


class _ContractState(Enum):
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
    lhs = values[0] != 0.0
    rhs = values[1] != 0.0
    return switch_(
        {
            _ContractState.ALL_KEYS_MATCH: lambda k, v: convert[TSD](keys[0], sum_(values, 0.0)),
            _ContractState.LEFT_ONLY: lambda k, v: convert[TSD](k[0], v[0]),
            _ContractState.RIGHT_ONLY: lambda k, v: convert[TSD](k[1], v[1]),
            _ContractState.OTHERWISE: lambda k, v: merge(convert[TSD](k[0], v[0]), convert[TSD](k[1], v[1]),
                                                         disjoint=True),
        },
        if_then_else(match, _ContractState.ALL_KEYS_MATCH,
                     if_then_else(lhs, _ContractState.LEFT_ONLY,
                                  if_then_else(rhs, _ContractState.RIGHT_ONLY, _ContractState.OTHERWISE))),
        keys,
        values
    )
