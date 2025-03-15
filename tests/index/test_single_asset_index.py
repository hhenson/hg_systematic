from datetime import date, datetime
from importlib import resources as pkg_resources

import polars as pl
import polars.selectors as cs
from frozendict import frozendict
from hgraph import graph, register_service, default_path, TSB

from hg_systematic.impl import trade_date_week_days, calendar_for_static, create_market_holidays, \
    price_in_dollars_static_impl
from hg_systematic.index.pricing_service import IndexResult, price_index_op
from hg_systematic.index.single_asset_index import MonthlySingleAssetIndexConfiguration
from hg_systematic.operators import bbg_commodity_contract_fn

from hgraph.test import eval_node

@graph
def register_services():
    register_service(default_path, trade_date_week_days)
    register_service(
        default_path, calendar_for_static,
        holidays=frozendict({
            "BCOM": create_market_holidays(["US"], date(2018,1,1), date(2030,1,1)),
            "CL NonTrading": create_market_holidays(["US"], date(2018,1,1), date(2030,1,1)),
        }),
    )
    import tests.index
    with pkg_resources.path(tests.index, "CL.parquet") as file:
        cl_df = pl.read_parquet(file)
    prcs = cl_df.unpivot(cs.numeric(), index="date",variable_name="symbol", value_name="price")
    register_service(default_path, price_in_dollars_static_impl, prices=prcs)


def test_single_asset_index():

    @graph
    def g() -> TSB[IndexResult]:
        register_services()
        return price_index_op(
            config=MonthlySingleAssetIndexConfiguration(
                symbol="CL Index",
                publish_holiday_calendar="BCOM",
                rounding=8,
                initial_level=100.0,
                start_date=date(2025, 4, 1),
                asset="CL",
                roll_period=[5, 10],
                roll_schedule=["H0", "H0", "K0", "K0", "N0", "N0", "U0", "U0", "X0", "X0", "F0", "F1"],
                trading_halt_calendar="CL NonTrading",
                contract_fn=bbg_commodity_contract_fn
            ))

    result = eval_node(g, __start_time__=datetime(2025, 4, 1), __end_time__=datetime(2025, 5, 1), __elide__=True)
    print('Result', result)
    assert result

