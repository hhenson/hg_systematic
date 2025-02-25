from datetime import datetime

from hgraph import TS, cmp_, TSB, CmpResult, graph, register_service, default_path
from hgraph.test import eval_node

from hg_systematic.impl import calendar_for_static, business_day_impl, trade_date_week_days, \
    monthly_rolling_weights_impl
from hg_systematic.operators import MonthlyRollingRange, monthly_rolling_weights, MonthlyRollingWeightRequest

from frozendict import frozendict as fd


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
        CmpResult.LT, CmpResult.EQ, None, None, CmpResult.GT, CmpResult.LT
    ]


@graph
def monthly_roll(request: TS[MonthlyRollingWeightRequest]) -> TS[float]:
    register_service(default_path, calendar_for_static, holidays=fd(Test=frozenset()))
    register_service(default_path, business_day_impl)
    register_service(default_path, trade_date_week_days)
    register_service(default_path, monthly_rolling_weights_impl)
    return monthly_rolling_weights(request)


def test_monthly_rolling_range_positive():
    assert eval_node(
        monthly_roll,
        [MonthlyRollingWeightRequest(calendar_name="Test", round_to=2, start=5, end=10)],
        __start_time__=datetime(2025, 1, 2),
        __end_time__=datetime(2025, 1, 31),
        __elide__=True
    ) == [
               1.0, 0.8, 0.6, 0.4, 0.2, 0.0, 1.0
           ]


def test_monthly_roll_range_negative():
    assert eval_node(
        monthly_roll,
        [MonthlyRollingWeightRequest(calendar_name="Test", round_to=2, start=-2, end=3)],
        __start_time__=datetime(2025, 1, 15),
        __end_time__=datetime(2025, 2, 15),
        __elide__=True
    ) == [
               1.0, 0.8, 0.6, 0.4, 0.2, 0.0, 1.0
           ]
