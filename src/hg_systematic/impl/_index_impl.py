from typing import Mapping

from hgraph import service_impl, set_service_output, const, TSD, TSS, TSL, TS, Size, get_service_inputs, map_

from hg_systematic.operators import index_assets, index_composition
from hg_systematic.operators._index import index_rolling_schedule, ROLLING_SCHEDULE

__all__ = ["index_impl_const", "index_rolling_schedule_impl_const"]


@service_impl(interfaces=(index_assets, index_composition))
def index_impl_const(path: str, assets_and_weights: Mapping[str, Mapping[str, float]]):
    assets_and_weights = const(assets_and_weights, tp=TSD[str, TSD[str, TS[float]]])

    asset_requests = get_service_inputs(path, index_assets).symbol
    asset_responses = map_(lambda x: x.key_set, assets_and_weights, __keys__=asset_requests)
    set_service_output(path, index_assets, asset_responses)

    weight_requests = get_service_inputs(path, index_composition).symbol
    weight_responses =  map_(lambda x: x, assets_and_weights, __keys__=weight_requests)
    set_service_output(path, index_composition, weight_responses)


@service_impl(interfaces=(index_rolling_schedule,))
def index_rolling_schedule_impl_const(
        symbol: TSS[str],
        rolling_data: Mapping[str, Mapping[str, Mapping[int, tuple[int, int]]]],
        tp: type[ROLLING_SCHEDULE] = TSD[str, TSD[int, TSL[TS[int], Size[2]]]]
) -> TSD[str, ROLLING_SCHEDULE]:
    """
    Takes in a map of asset -> symbol -> month -> (contract month, contract year offset).
    This is effectively reference data, but could be implemented as a dynamic
    load from a database as well and could theoretically be PIT data as well.
    """
    return const(rolling_data, tp=TSD[str, TSD[str, TSD[int, TSL[TS[int], Size[2]]]]])
