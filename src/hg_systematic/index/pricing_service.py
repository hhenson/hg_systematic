from dataclasses import dataclass

from hgraph import subscription_service, TSS, TS, TSD, mesh_, graph, service_impl, dispatch, operator, \
    TimeSeriesSchema, TSB, default_path, gate, get_mesh

from hg_systematic.index.configuration import IndexConfiguration
from hg_systematic.index.configuration_service import index_configuration
from hg_systematic.index.units import IndexStructure

__all__ = ["price_index", "price_index_service", "price_index_impl", "INDEX_MESH", "IndexResult", "price_index_op"]

from hg_systematic.operators import trade_date

INDEX_MESH = "index_mesh"


@dataclass
class IndexResult(TimeSeriesSchema):
    level: TS[float]
    index_structure: TSB[IndexStructure]


@graph
def price_index(symbol: TS[str], path: str = default_path) -> TSB[IndexResult]:
    """
    To ensure that we use the mesh inside of a pricing service and the service when outside, we
    use this wrapper.
    """
    if m := get_mesh(INDEX_MESH):
        return m[symbol]
    else:
        return price_index_service(symbol)


@subscription_service
def price_index_service(symbol: TS[str], path: str = default_path) -> TSB[IndexResult]:
    """
    Produce a price for an index.
    """


@service_impl(interfaces=price_index_service)
def price_index_impl(symbol: TSS[str]) -> TSD[str, TSB[IndexResult]]:
    """
    The basic structure for implementing the index pricing service. This makes use of the mesh_ operator allowing
    for nested pricing structures.
    """
    return _price_index_mesh(symbol)


@graph
def _price_index_mesh(symbol: TSS[str]) -> TSD[str, TSB[IndexResult]]:
    """Separate the mesh impl to make testing easier."""
    return mesh_(
        _price_index,
        __keys__=symbol,
        __key_arg__="symbol",
        __name__=INDEX_MESH
    )


@graph
def _price_index(symbol: TS[str]) -> TSB[IndexResult]:
    """Loads the index configuration object and dispatches it"""
    config = index_configuration(symbol)
    # Ensure we only start trying to compute the index once the start date
    # is achieved or past.
    dt = trade_date()  # We expect the set of trade dates to be larger than the set of publishing dates.
    config = gate(dt >= config.start_date, config, -1)
    return price_index_op(config)


@dispatch(on=("config",))
@operator
def price_index_op(config: TS[IndexConfiguration]) -> TSB[IndexResult]:
    """
    Dispatches to the appropriate pricing implementation based on the configuration instance.
    To implement an index, implement the price_index_op operator.
    """
