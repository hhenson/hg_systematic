from hgraph import TS, cmp_, TSB, CmpResult, graph
from hgraph.test import eval_node

from hg_systematic.operators._rolling_rules import MonthlyRollingRange


def test_cmp_monthly_rolling_range():

    @graph
    def g(date_index: TS[int], start: TS[int], end: TS[int], first_day: TS[int]) -> TS[CmpResult]:
        return cmp_(date_index, TSB[MonthlyRollingRange].from_ts(
            start=start, end=end, first_day=first_day,
        ))

    assert eval_node(g, [1, 4, 9, 10], [4], [9], [4]) == [
        CmpResult.LT, CmpResult.EQ, CmpResult.GT, CmpResult.LT
    ]

    assert eval_node(g, [17, 18, 22, 2, 3, 4], [-4], [3], [18]) == [
        CmpResult.LT, CmpResult.EQ, CmpResult.EQ, CmpResult.EQ, CmpResult.GT, CmpResult.LT
    ]
