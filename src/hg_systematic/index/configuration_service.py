from typing import Mapping

from hgraph import subscription_service, TSS, TS, service_impl, const, TSD, map_

from hg_systematic.index.configuration import IndexConfiguration

__all__ = ["index_configuration", "static_index_configuration", ]


@subscription_service
def index_configuration(symbol: TS[str]) -> TS[IndexConfiguration]:
    """
    Retrieves the definition of the desired index.
    """


@service_impl(interfaces=index_configuration)
def static_index_configuration(symbol: TSS[str], indices: Mapping[str, IndexConfiguration]) \
        -> TSD[str, TS[IndexConfiguration]]:
    """
    Provide a simple implementation of the index configuration service that supports a manual mapping of symbol to
    configurations.
    """
    indices = const(indices, TSD[str, TS[IndexConfiguration]])
    return map_(lambda config: config, indices, __key__=symbol)
