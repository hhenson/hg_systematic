from datetime import date, timedelta

from hgraph import graph, register_service, default_path, MIN_ST, TS
from hgraph.test import eval_node

from hg_systematic.impl import price_in_dollars_static_impl
import polars as pl

from hg_systematic.index.configuration import StubIndexConfiguration
from hg_systematic.index.stub_index import price_stub_index


def test_stub_index():
    @graph
    def g() -> TS[float]:
        prcs = pl.DataFrame(
            {"date": [MIN_ST.date() + timedelta(days=1)], "symbol": ["TestInstrument"], "price": [100.0]})
        register_service(default_path, price_in_dollars_static_impl, prices=prcs, round_to=2)
        return price_stub_index(StubIndexConfiguration(symbol="TestInstrument")).level

    assert eval_node(g, __elide__=True) == [100.0]
