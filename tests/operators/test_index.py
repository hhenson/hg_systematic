from datetime import date

import pytest
from frozendict import frozendict as fd
from hgraph import SIZE, Size, graph, TSL, TS, TSD, const, register_service, default_path
from hgraph.test import eval_node

from hg_systematic.impl import trade_date_week_days, calendar_for_static, index_impl_const, \
    index_rolling_schedule_impl_const
from hg_systematic.operators import index_rolling_weights, HolidayCalendar
from hg_systematic.operators._index import _to_tsd, index_weights, index_rolling_contracts, DEFAULT_INDEX_PATH


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
    assert eval_node(_to_tsd[SIZE: Size[2]], [keys], [values]) == [expected]


def test_bcom_rolling_rule():
    @graph
    def g(dt: TS[date]) -> TSL[TS[float], Size[2]]:
        return index_rolling_weights("BCOM",
                                     const(fd({"holidays": frozenset({date(2025, 1, 1)}),
                                               "start_of_week": 0,
                                               "end_of_week": 4}), tp=HolidayCalendar),
                                     dt)

    assert eval_node(
        g,
        [
            date(2025, 1, 8),
            date(2025, 1, 9),
            date(2025, 1, 14),
            date(2025, 1, 15),
        ]
    ) == [
               {0: 1.0, 1: 0.0},
               {0: 0.8, 1: 0.2},
               {0: 0.2, 1: 0.8},
               {0: 0.0, 1: 1.0},
           ]


def test_bcom_rolling_contracts():
    @graph
    def g(symbol: TS[str], dt: TS[date]) -> TSL[TS[str], Size[2]]:
        register_service(DEFAULT_INDEX_PATH,
                         index_rolling_schedule_impl_const,
                         rolling_data=fd({
                             "BCOM Index": fd({
                                 "C": fd({1: (3, 0), 2: (3, 0), 10: (12, 0), 11: (12, 0)}),
                                 "GC": fd({1: (2, 0), 2: (4, 0), 10: (12, 0), 11: (12, 0)}),
                                 "HG": fd({1: (3, 0), 2: (3, 0), 10: (12, 0), 11: (12, 0)}),
                                 "CL": fd({1: (3, 0), 2: (3, 0), 10: (11, 0), 11: (1, 1)})
                             })
                         }))
        return index_rolling_contracts("BCOM",
                                       symbol,
                                       dt)

    assert eval_node(
        g,

        ["GC", "CL"],
        [
            date(2025, 1, 8),
            date(2025, 10, 9),
        ]
    ) == [
               {0: "GCG25 Comdty", 1: "GCJ25 Comdty"},
               {0: "CLX25 Comdty", 1: "CLF26 Comdty"},
           ]


def test_index_weights():
    @graph
    def g(dt: TS[date]) -> TSD[str, TS[float]]:
        register_service(default_path, trade_date_week_days)
        register_service(DEFAULT_INDEX_PATH, index_impl_const,
                         assets_and_weights=fd({"BCOM Index": fd({"C": 0.25, "GC": 0.2, "HG": 0.4, "CL": 0.15})}))
        register_service(default_path, calendar_for_static,
                         holidays=fd({"BCOM Index": frozenset({date(2025, 1, 1)})}), )
        register_service(DEFAULT_INDEX_PATH,
                         index_rolling_schedule_impl_const,
                         rolling_data=fd({
                             "BCOM Index": fd({
                                 "C": fd({1: (3, 0), 2: (3, 0), 10: (12, 0), 11: (12, 0)}),
                                 "GC": fd({1: (2, 0), 2: (4, 0), 10: (12, 0), 11: (12, 0)}),
                                 "HG": fd({1: (3, 0), 2: (3, 0), 10: (12, 0), 11: (12, 0)}),
                                 "CL": fd({1: (3, 0), 2: (3, 0), 10: (11, 0), 11: (1, 1)})
                             })
                         }))
        out = index_weights("BCOM Index",
                            dt,
                            "BCOM")
        return out

    assert eval_node(
        g,
        [
            date(2025, 1, 8),
            None,  # Let subscriptions catch up
            date(2025, 1, 9),
            date(2025, 1, 14),
            date(2025, 1, 15),
        ],
    ) == [
               None,
               fd({'CH25 Comdty': 0.25, 'HGH25 Comdty': 0.4, 'GCG25 Comdty': 0.2, 'CLH25 Comdty': 0.15}),
               fd(
                   {'GCG25 Comdty': 0.16000000000000003, 'CH25 Comdty': 0.25, 'HGH25 Comdty': 0.4000000000000001,
                    'CLH25 Comdty': 0.15, 'GCJ25 Comdty': 0.04000000000000001}),
               fd(
                   {'GCJ25 Comdty': 0.16000000000000003, 'GCG25 Comdty': 0.04000000000000001, 'CH25 Comdty': 0.25,
                    'HGH25 Comdty': 0.4000000000000001, 'CLH25 Comdty': 0.15}),
               fd(
                   {'GCJ25 Comdty': 0.2, 'GCG25 Comdty': 0.0, 'CH25 Comdty': 0.25, 'HGH25 Comdty': 0.4,
                    'CLH25 Comdty': 0.15})
           ]
