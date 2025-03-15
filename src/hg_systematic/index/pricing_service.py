from dataclasses import dataclass

from hgraph import subscription_service, TSS, TS, TSD, mesh_, graph, service_impl, dispatch_, dispatch, operator, \
    TimeSeriesSchema, TSB

from hg_systematic.index.configuration import IndexConfiguration
from hg_systematic.index.configuration_service import index_configuration
from hg_systematic.index.units import IndexStructure

__all__ = ["price_index", "price_index_impl", "INDEX_MESH", "IndexResult", "price_index_op"]


@dataclass
class IndexResult(TimeSeriesSchema):
    level: TS[float]
    index_structure: TSB[IndexStructure]


@subscription_service
def price_index(symbol: TS[str]) -> TSB[IndexResult]:
    """
    Produce a price for an index.
    """


INDEX_MESH = "index_mesh"


@service_impl(interfaces=price_index)
def price_index_impl(symbol: TSS[str]) -> TSD[str, TSB[IndexResult]]:
    """
    The basic structure for implementing the index pricing service. This makes use of the mesh_ operator allowing
    for nested pricing structures.
    """
    return mesh_(
        _price_index,
        __key__=symbol,
        __name__=INDEX_MESH
    )


@graph
def _price_index(symbol: TS[str]) -> TSB[IndexResult]:
    """Loads the index configuration object and dispatches it"""
    config = index_configuration(symbol)
    return price_index_op(config)


@dispatch(on=("config",))
@operator
def price_index_op(config: TS[IndexConfiguration]) -> TSB[IndexResult]:
    """
    Dispatches to the appropriate pricing implementation based on the configuration instance.
    To implement an index, implement the price_index_op operator.
    """
