from dataclasses import dataclass
from typing import Callable, TypeVar

from hgraph import TS, TSB, graph, map_, mesh_, TS_SCHEMA, AUTO_RESOLVE, convert, TSS, TSD, compute_node

from hg_systematic.index.configuration import MultiIndexConfiguration
from hg_systematic.index.index_utils import DebugContext, monthly_rolling_index
from hg_systematic.index.pricing_service import IndexResult, INDEX_MESH, price_index_op
from hg_systematic.index.units import NotionalUnitValues

__all__ = [
    "price_monthly_multi_index", "multi_index_monthly_rolling_index", "get_sub_levels", "set_multi_index_debug_on",
    "MonthlyRollingMultiIndexConfiguration", "MonthlyRollingMultiIndexFixedWeightConfiguration",
    "AnnualMultiIndexConfiguration", "ROLLING_MULTI_CONFIG", "compute_target_units_multi_index"
]

DEBUG_ON = False


def set_multi_index_debug_on():
    global DEBUG_ON
    DEBUG_ON = True


@dataclass(frozen=True)
class MonthlyRollingMultiIndexConfiguration(MultiIndexConfiguration):
    roll_period: tuple[int, int] = None
    roll_rounding: int = 8


@dataclass(frozen=True)
class MonthlyRollingMultiIndexFixedWeightConfiguration(MonthlyRollingMultiIndexConfiguration):
    weights: tuple[float, ...] = None


@dataclass(frozen=True)
class AnnualMultiIndexConfiguration(MonthlyRollingMultiIndexConfiguration):
    # Re-balances once a year on the month indicated, using the defined roll schedule
    re_balance_month: int = None


ROLLING_MULTI_CONFIG = TypeVar("ROLLING_MULTI_CONFIG", bound=MultiIndexConfiguration)


# @graph(overloads=price_index_op)
# def price_monthly_multi_index(config: TS[ROLLING_MULTI_CONFIG]) -> TSB[IndexResult]:
#     """Can price all monthly rolling indices."""
#     with DebugContext(label="[MonthlyRollingMultiIndexConfiguration]", debug=DEBUG_ON):
#         monthly_rolling_index(
#             config=config,
#         )

@graph(overloads=price_index_op)
def price_monthly_multi_index(config: TS[MonthlyRollingMultiIndexFixedWeightConfiguration]) -> TSB[IndexResult]:
    """Can price all monthly rolling indices."""
    with DebugContext(prefix="[MonthlyRollingMultiIndexFixedWeightConfiguration]", debug=DEBUG_ON):
        return multi_index_monthly_rolling_index(
            config=config,
            weights_fn=_fixed_weight_fn,
        )


@compute_node
def _fixed_weight_fn(config: TS[MonthlyRollingMultiIndexFixedWeightConfiguration], sub_levels: NotionalUnitValues) -> TSD[str, TS[float]]:
    config = config.value
    return {k: v for k, v in zip(config.indices, config.weights)}


@graph
def multi_index_monthly_rolling_index(
        config: TS[ROLLING_MULTI_CONFIG],
        weights_fn: Callable[[TS[ROLLING_MULTI_CONFIG], NotionalUnitValues], TS[float]],
        re_balance_signal_fn: Callable[[TSB[TS_SCHEMA]], TS[bool]] = None,
        _cfg_tp: type[ROLLING_MULTI_CONFIG] = AUTO_RESOLVE,
) -> TSB[IndexResult]:
    sub_levels = get_sub_levels(config)

    return monthly_rolling_index(
        config=config,
        prices=sub_levels,
        compute_target_units_fn=compute_target_units_multi_index,
        re_balance_signal_fn=re_balance_signal_fn,
        target_weights=weights_fn(config, sub_levels),
    )


@graph
def get_sub_levels(config: TS[ROLLING_MULTI_CONFIG]) -> NotionalUnitValues:
    """A multi-index obtains does not need to track contracts.
    We also make the simplification that the sub-indices are constant over time. This is not strictly necessary,
    but we can always increase the items in the config and supply zero weights for the new items in the past.
    """
    # For now assume that the underlying indices operate on the same publishing calendar as the outer index
    # i.e. we do not filter or sample as we have done in the single asset index. This could be done if the
    # requirement was identified.

    # We are using the mesh_ component to price, this requires the mesh_ is initialised elsewhere, and we are
    # assuming this is how the main multi-index instrument was constructed.
    # The mesh_ instance is retrieved by name, the INDEX_MESH is the name we expect to be allocated to the main
    # mesh_ instance. We are only interested in the level, so just reference that.
    DebugContext.print("[pricing] Requesting", (keys:=convert[TSS[str]](config.indices)))
    levels = map_(lambda key: mesh_(INDEX_MESH)[key].level, __keys__=keys)
    DebugContext.print("[pricing] sub-levels", levels)
    return levels


@graph
def compute_target_units_multi_index(tsb: TSB[TS_SCHEMA]) -> NotionalUnitValues:
    """
    Since a multi-index does not cycle contracts (as is the case in single asset indices) we just
    need a weight for the re-balance date then we construct the target units from that.
    :param tsb: Must contain target_weights.
    """
    level = tsb.level  # TS[float]
    sub_index_levels = tsb.prices  # TSB[str, TS[float]
    target_weights = tsb.target_weights  # TSB[str, TS[float]]
    DebugContext.print("[compute_target_units_multi_index] level", level)
    DebugContext.print("[compute_target_units_multi_index] sub_index_levels", sub_index_levels)
    DebugContext.print("[compute_target_units_multi_index] target_weights", target_weights)
    out = map_(
        lambda l, s_i_l, t_w: (l * t_w) / s_i_l,
        level,
        sub_index_levels,
        target_weights,
        __keys__=convert[TSS[str]](tsb.config.indices)
    )
    DebugContext.print("[compute_target_units_multi_index] target_units", out)
    return out
