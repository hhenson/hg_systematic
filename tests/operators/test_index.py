import pytest
from hgraph import SIZE, Size, graph, TSL, TS, TSD
from hgraph.test import eval_node
from hg_systematic.operators._index import _to_tsd
from frozendict import frozendict as fd


@pytest.mark.parametrize(
    ["keys", "values", "expected"],
    [
        [("k1", "k2"), (1.0, 0.0), fd(k1=1.0)],
        [("k1", "k2"), (0.0, 1.0), fd(k2=1.0)],
        [("k1", "k1"), (0.3, 0.7), fd(k1=1.0)],
        [("k1", "k2"), (0.7, 0.3), fd(k1=0.7, k2=0.3)],
    ]
)
def test_to_tsd(keys, values, expected):
    eval_node(_to_tsd[SIZE: Size[2]], [keys], [values]) == [expected]
