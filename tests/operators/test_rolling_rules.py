from datetime import datetime, date

import pytest
from frozendict import frozendict as fd
from hgraph import TS, cmp_, TSB, CmpResult, graph, register_service, default_path, TSL, Size, combine
from hgraph.test import eval_node

from examples.bcom_index.bcom_index import get_bcom_roll_schedule, create_bcom_holidays
from hg_systematic.impl import calendar_for_static, business_day_impl, trade_date_week_days, \
    monthly_rolling_weights_impl, rolling_schedules_service_impl
from hg_systematic.impl._rolling_rules_impl import monthly_rolling_info_service_impl
from hg_systematic.operators import MonthlyRollingRange, monthly_rolling_weights, MonthlyRollingWeightRequest
from hg_systematic.operators._rolling_rules import futures_rolling_contracts, rolling_schedules, \
    bbg_commodity_contract_fn, monthly_rolling_info, MonthlyRollingRequest


@graph
def cmp_mrr(date_index: TS[int], start: TS[int], end: TS[int], first_day: TS[int]) -> TS[CmpResult]:
    return cmp_(date_index, TSB[MonthlyRollingRange].from_ts(
        start=start, end=end, first_day=first_day,
    ))


def test_cmp_monthly_rolling_range_positive():
    assert eval_node(cmp_mrr, [1, 6, 10, 11], [5], [10], [4]) == [
        CmpResult.LT, CmpResult.EQ, CmpResult.GT, CmpResult.LT
    ]


def test_cmp_monthly_rolling_range_negative():
    assert eval_node(cmp_mrr, [17, 22, 2, 3, 4], [-5], [3], [18]) == [
        CmpResult.LT, CmpResult.EQ, None, CmpResult.GT, CmpResult.LT
    ]


@graph
def monthly_roll(request: TS[MonthlyRollingWeightRequest]) -> TS[float]:
    register_service(default_path, calendar_for_static, holidays=fd(Test=frozenset()))
    register_service(default_path, business_day_impl)
    register_service(default_path, trade_date_week_days)
    register_service(default_path, monthly_rolling_weights_impl)
    register_service(default_path, monthly_rolling_info_service_impl)
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


@graph
def roll_contracts_(
        start: TS[int],
        end: TS[int],
) -> TSL[TS[str], Size[2]]:
    rs = get_bcom_roll_schedule()
    register_service(default_path, rolling_schedules_service_impl, schedules=rs)
    register_service(default_path, monthly_rolling_info_service_impl)
    register_service(default_path, business_day_impl)
    register_service(default_path, trade_date_week_days)
    register_service(default_path, calendar_for_static, holidays=fd(BCOM=create_bcom_holidays()))

    request = combine[TS[MonthlyRollingRequest]](start=start, end=end, calendar_name="BCOM")
    rolling_info = monthly_rolling_info(request)
    contracts = futures_rolling_contracts(
        rolling_info,
        rolling_schedules()['GC'],
        'GC',
        bbg_commodity_contract_fn
    )
    return contracts


@pytest.mark.parametrize(
    ['dt', 'expected'],
    [
        [date(2025, 1, 8), {0: "GCG25 Comdty", 1: "GCJ25 Comdty"}],
        [date(2025, 12, 9), {0: "GCG26 Comdty", 1: "GCG26 Comdty"}],
    ]
)
def test_roll_contracts_monthly_no_range(dt, expected):
    assert eval_node(
        roll_contracts_,
        [5],
        [10],
        __elide__=True,
        __start_time__=datetime(dt.year, dt.month, dt.day),
        __end_time__=datetime(dt.year, dt.month, dt.day, 23),
    ) == [expected]


@pytest.mark.parametrize(
    ['dt', 'expected'],
    [
        [date(2024, 12, 9), {0: "GCG25 Comdty", 1: "GCJ25 Comdty"}],
        [date(2024, 12, 30), {0: "GCG25 Comdty", 1: "GCJ25 Comdty"}],
        [date(2025, 1, 2), {0: "GCG25 Comdty", 1: "GCJ25 Comdty"}],
        [date(2025, 1, 17), {0: "GCJ25 Comdty", 1: "GCJ25 Comdty"}],
    ]
)
def test_roll_contracts_monthly_with_range(dt, expected):
    assert eval_node(
        roll_contracts_,
        [-3],
        [5],
        __elide__=True,
        __start_time__=datetime(dt.year, dt.month, dt.day),
        __end_time__=datetime(dt.year, dt.month, dt.day, 23),
    ) == [expected]

# def test_rolling_contract_for():
#     @graph
#     def g() -> TSL[TS[str], Size[2]]:
#         register_service(default_path, calendar_for_static, holidays=fd(GC=frozenset()))
#         register_service(default_path, business_day_impl)
#         register_service(default_path, trade_date_week_days)
#         rs = get_bcom_roll_schedule()
#         register_service(
#             default_path,
#             rolling_contracts_for_impl,
#             roll_schedule=rs,
#             format_str=fd({k: f"{k}{{month}}{{year:02d}} Comdty" for k in rs.keys()}),
#             year_scale=fd({k: 100 for k in rs.keys()}),
#             dt_symbol=fd({k: "GC" for k in rs.keys()})
#         )
#         return rolling_contracts_for("GC")
#
#     assert eval_node(
#         g,
#         __start_time__=datetime(2025, 1, 2),
#         __end_time__=datetime(2025, 1, 4),
#         __elide__=True,
#     ) == [{0: "GCG25 Comdty", 1: "GCJ25 Comdty"}]
