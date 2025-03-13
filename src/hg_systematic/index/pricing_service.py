from hgraph import subscription_service, TSS, TS, TSD, mesh_, graph, service_impl, dispatch_, dispatch, operator

from hg_systematic.index.configuration import IndexConfiguration
from hg_systematic.index.configuration_service import index_configuration


@subscription_service
def price_index(symbol: TSS[str]) -> TS[float]:
    """
    Produce a price for an index.
    """

INDEX_MESH = "index_mesh"

@service_impl(interfaces=price_index)
def price_index_impl(symbol: TSS[str]) -> TSD[str, TS[float]]:
    """
    The basic structure for implementing the index pricing service. This makes use of the mesh_ operator allowing
    for nested pricing structures.
    """
    return mesh_(
        _price_index,
        __key__ = symbol,
        __name__ = INDEX_MESH
    )


@graph
def _price_index(symbol: TS[str]) -> TS[float]:
    """Loads the index configuration object and dispatches it"""
    config = index_configuration(symbol)
    return price_index_op(config)


@dispatch(on=("config",))
@operator
def price_index_op(config: TS[IndexConfiguration]) -> TS[float]:
    """
    Dispatches to the appropriate pricing implementation based on the configuration instance.
    To implement an index, implement the price_index_op operator.
    """
