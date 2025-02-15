from datetime import date

import pytest
from frozendict import frozendict as fd
from hgraph import SIZE, Size, graph, TSL, TS, TSD, const, register_service, default_path, debug_print
from hgraph.test import eval_node

from examples.bcom_index.bcom_index import create_bcom_holidays
from hg_systematic.impl import trade_date_week_days, calendar_for_static, holiday_const, create_market_holidays
from hg_systematic.operators import index_rolling_weight, index_rolling_contracts, INDEX_ROLL_STR, index_composition


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


def test_bcom_rolling_contracts():
    @graph
    def g(dt: TS[date]) -> TSL[TS[str], Size[2]]:
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
            date(2025, 1, 8),
            date(2025, 12, 9),
        ]
    ) == [
               {0: "GCG25 Comdty", 1: "GCJ25 Comdty"},
               {0: "GCG26 Comdty", 1: "GCG26 Comdty"},
           ]


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
