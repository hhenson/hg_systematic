from datetime import datetime, date, timedelta

from hgraph.test import eval_node

from hg_systematic.operators import business_days, Periods

def test_business_days():
    dt = date(2025, 1, 20)
    eval_node(
        business_days,
        [Periods.Week],
        [{"holidays": frozenset(), "start_of_week": 0, "end_of_week": 4}],
        __start_time__=datetime(2025, 1, 23)
    ) == [tuple(dt + timedelta(days=i) for i in range(7))]

