from datetime import date, datetime

import pytest
from frozendict import frozendict as fd
from hgraph import SIZE, Size, graph, TSL, TS, TSD, const, register_service, default_path, debug_print, lift
from hgraph.test import eval_node

from examples.bcom_index.bcom_index import create_bcom_holidays, load_sample_prices
from hg_systematic.impl import trade_date_week_days, calendar_for_static, holiday_const, create_market_holidays, \
    business_day_impl, price_in_dollars_static_impl, monthly_rolling_info_service_impl
from hg_systematic.operators import index_rolling_weight, index_rolling_contracts, INDEX_ROLL_STR, index_composition, \
    index_level
import polars as pl


def test_bcom_rolling_rule():
    @graph
    def g(dt: TS[date]) -> TS[float]:
        return index_rolling_weight(
            "BCOM Index",
            dt,
            holiday_const(create_bcom_holidays())
        )

    assert eval_node(
        g,
        [
            date(2025, 1, 8),
            date(2025, 1, 9),
            date(2025, 1, 10),
            date(2025, 1, 13),
            date(2025, 1, 14),
            date(2025, 1, 15),
        ]
    ) == [1.0, 0.8, 0.6, 0.4, 0.2, 0.0]


@pytest.mark.parametrize(
    ["dt", "expected"],
    [
        [date(2025, 1, 8), {0: "GCG25 Comdty", 1: "GCJ25 Comdty"}],
        [date(2025, 12, 9), {0: "GCG26 Comdty", 1: "GCG26 Comdty"}],
    ]
)
def test_bcom_rolling_contracts(dt, expected, ):
    @graph
    def g(dt: TS[date]) -> TSL[TS[str], Size[2]]:
        register_service(default_path, monthly_rolling_info_service_impl)
        register_service(default_path, business_day_impl)
        register_service(default_path, trade_date_week_days)
        register_service(default_path, calendar_for_static, holidays=fd(BCOM=create_bcom_holidays()))

        contracts = index_rolling_contracts(
            "BCOM Index",
            dt,
            holiday_const(create_bcom_holidays()),
        )
        first = contracts.first
        second = contracts.second
        return TSL.from_ts(first["GC"], second["GC"])

    assert eval_node(
        g,
        [
            dt
        ],
        __elide__=True,
        __start_time__=datetime(dt.year, dt.month, dt.day),
        __end_time__=datetime(dt.year, dt.month, dt.day, 23),
    ) == [expected]


def test_index_weights():
    @graph
    def g(dt: TS[date]) -> TSL[TS[float], Size[2]]:
        out = index_composition(
            "BCOM Index",
            dt,
            holiday_const(create_bcom_holidays())
        )
        return TSL.from_ts(out.first['GC'], out.second['GC'])

    assert eval_node(
        g,
        [
            date(2025, 1, 8),
            date(2025, 2, 5),
        ],
    ) == [
               {0: 0.3334984, 1: 0.27352246},
               {0: 0.27352246}
           ]

# def test_index_level():
#     @graph
#     def g() -> TS[float]:
#         register_service(default_path, trade_date_week_days)
#         register_service(default_path, business_day_impl)
#         register_service(default_path, calendar_for_static,
#                          holidays=fd({"BCOM Index": create_bcom_holidays()}))
#         register_service(default_path, price_in_dollars_static_impl, prices=load_sample_prices())
#         return index_level(
#             "BCOM Index",
#             rounding_fn=lift(lambda x: round(x, 8), inputs={'x': TS[float]}, output=TS[float])
#         )
#
#     assert eval_node(
#         g,
#         __start_time__=datetime(2025, 1, 2),
#         __end_time__=datetime(2025, 1, 4),
#         __elide__=True,
#     ) == [
#
#     ]
