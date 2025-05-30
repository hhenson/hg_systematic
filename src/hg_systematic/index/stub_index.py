from hgraph import graph, TS, TSB, combine, nothing, round_

from hg_systematic.index.configuration import StubIndexConfiguration
from hg_systematic.index.pricing_service import price_index_op, IndexResult
from hg_systematic.index.units import IndexStructure
from hg_systematic.operators import price_in_dollars


__all__ = ["price_stub_index"]


@graph(overloads=price_index_op)
def price_stub_index(config: TS[StubIndexConfiguration]) -> TSB[IndexResult]:
    """Returns the level but not the index structure."""
    return combine[TSB[IndexResult]](
        level=round_(price_in_dollars(config.symbol), config.rounding),  # Technically, a level is not a price in dollars but will do for now.
        index_structure=nothing[TSB[IndexStructure]]()
    )