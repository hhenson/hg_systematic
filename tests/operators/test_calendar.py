from datetime import datetime, date, timedelta
from frozendict import frozendict as fd

import pytest
from hgraph import graph, TSB, TS, contains_, register_service, default_path, service_impl
from hgraph.test import eval_node

from hg_systematic.impl import trade_date_week_days, business_day_impl, calendar_for_static, day_index_for_impl
from hg_systematic.operators import business_days, Periods, HolidayCalendarSchema, business_day
from hg_systematic.operators._calendar import next_month, day_index_for


def test_business_days_with_dt():
    dt = date(2025, 1, 20)
    assert eval_node(
        business_days,
        [Periods.Week],
        [{"holidays": frozenset(), "start_of_week": 0, "end_of_week": 4}],
        [dt, dt + timedelta(days=1), dt + timedelta(days=2), dt + timedelta(days=7)],
    ) == [tuple(dt + timedelta(days=i) for i in range(5)), None, None,
          tuple(dt + timedelta(days=7 + i) for i in range(5))]


@pytest.mark.parametrize(
    ["dt", "expected"],
    [
        [date(2025, 1, 1), True],
        [date(2025, 1, 4), True],
        [date(2025, 1, 6), False],
    ]
)
def test_in_calendar(dt, expected):
    @graph
    def g(c: TSB[HolidayCalendarSchema], d: TS[date]) -> TS[bool]:
        return contains_(c, d)

    eval_node(g, [{"holidays": frozenset({date(2025, 1, 1)}), "start_of_week": 0, "end_of_week": 4}], [dt]) == [
        expected]


def test_business_day():
    @graph
    def g() -> TS[date]:
        register_service(default_path, trade_date_week_days)
        register_service(default_path, business_day_impl)
        register_service(default_path, calendar_for_static, holidays=fd({"S1": frozenset({date(2025, 1, 1)})}), )
        return business_day("S1")

    assert eval_node(
        g,
        __start_time__=datetime(2025, 1, 1),
        __end_time__=datetime(2025, 1, 6, 23, 59),
        __elide__=True
    ) == [
               date(2025, 1, 2),
               date(2025, 1, 3),
               date(2025, 1, 6)
           ]


def test_next_month():

    assert eval_node(
        next_month,
        [date(2025, 1, 5), date(2024, 12, 20)]
    ) == [date(2025, 2, 1), date(2025, 1, 1)]


def test_day_index_for():
    @graph
    def g() -> TS[int]:
        register_service(default_path, trade_date_week_days)
        register_service(default_path, business_day_impl)
        register_service(default_path, calendar_for_static, holidays=fd({"S1": frozenset({date(2025, 1, 1)})}), )
        register_service(default_path, day_index_for_impl)
        return day_index_for("S1")

    assert eval_node(
        g,
        __start_time__=datetime(2025, 1, 1),
        __end_time__=datetime(2025, 1, 6, 23, 59),
        __elide__=True
    ) == [1, 2, 3]
