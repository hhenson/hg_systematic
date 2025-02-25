from datetime import datetime, date

from hgraph import TS, cmp_, TSB, CmpResult, graph, register_service, default_path, TSL, Size, const, TSD
from hgraph.test import eval_node

from examples.bcom_index.bcom_index import get_bcom_roll_schedule, create_bcom_holidays
from hg_systematic.impl import calendar_for_static, business_day_impl, trade_date_week_days, \
    monthly_rolling_weights_impl, holiday_const
from hg_systematic.impl._rolling_rules_impl import rolling_contracts_for_impl
from hg_systematic.operators import MonthlyRollingRange, monthly_rolling_weights, MonthlyRollingWeightRequest, \
    roll_contracts_monthly

from frozendict import frozendict as fd

from hg_systematic.operators._rolling_rules import rolling_contracts_for


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
        dt: TS[date],
        roll_index: TS[int] = None,
        roll_range: TSB[MonthlyRollingRange] = None
) -> TSL[TS[str], Size[2]]:
    rs = get_bcom_roll_schedule()
    roll_schedule = const(rs, TSD[str, TSD[int, TS[tuple[int, int]]]])
    fmt_str = const(fd({k: f"{k}{{month}}{{year:02d}} Comdty" for k in rs.keys()}), TSD[str, TS[str]])
    year_scale = const(fd({k: 100 for k in rs.keys()}), TSD[str, TS[int]])
    if roll_range:
        contracts = roll_contracts_monthly(
            dt,
            roll_schedule,
            fmt_str,
            year_scale,
            roll_index,
            roll_range,
        )
    else:
        contracts = roll_contracts_monthly(
            dt,
            roll_schedule,
            fmt_str,
            year_scale,
        )
    first = contracts.first
    second = contracts.second
    return TSL.from_ts(first["GC"], second["GC"])


def test_roll_contracts_monthly_no_range():
    assert eval_node(
        roll_contracts_,
        [
            date(2025, 1, 8),
            date(2025, 12, 9),
        ],
    ) == [
               {0: "GCG25 Comdty", 1: "GCJ25 Comdty"},
               {0: "GCG26 Comdty", 1: "GCG26 Comdty"},
           ]


def test_roll_contracts_monthly_with_range():
    assert eval_node(
        roll_contracts_,
        [
            date(2024, 12, 9),
            date(2024, 12, 30),
            date(2025, 1, 2),
            date(2025, 1, 20),
        ],
        [
            6, 20, 2, 14
        ],
        [
            fd(start=-3, end=5, first_day=18)
        ]
    ) == [
               {0: "GCG25 Comdty", 1: "GCJ25 Comdty"},
               None, None,
               {0: "GCJ25 Comdty", 1: "GCJ25 Comdty"},
           ]


def test_rolling_contract_for():
    @graph
    def g() -> TSL[TS[str], Size[2]]:
        register_service(default_path, calendar_for_static, holidays=fd(GC=frozenset()))
        register_service(default_path, business_day_impl)
        register_service(default_path, trade_date_week_days)
        rs = get_bcom_roll_schedule()
        register_service(
            default_path,
            rolling_contracts_for_impl,
            roll_schedule=rs,
            format_str=fd({k: f"{k}{{month}}{{year:02d}} Comdty" for k in rs.keys()}),
            year_scale=fd({k: 100 for k in rs.keys()}),
            dt_symbol=fd({k: "GC" for k in rs.keys()})
        )
        return rolling_contracts_for("GC")

    assert eval_node(
        g,
        __start_time__=datetime(2025, 1, 2),
        __end_time__=datetime(2025, 1, 4),
        __elide__=True,
    ) == [{0: "GCG25 Comdty", 1: "GCJ25 Comdty"}]