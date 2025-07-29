from datetime import date, datetime
from importlib import resources as pkg_resources

import polars as pl
import polars.selectors as cs
from frozendict import frozendict
from hgraph import graph, register_service, default_path, TSB, DebugContext

from hg_systematic.impl import trade_date_week_days, calendar_for_static, create_market_holidays, \
    price_in_dollars_static_impl, monthly_rolling_info_service_impl, monthly_rolling_weights_impl, business_day_impl
from hg_systematic.index.configuration_service import static_index_configuration
from hg_systematic.index.pricing_service import price_index_impl

from hg_systematic.index.single_asset_index import MonthlySingleAssetIndexConfiguration, \
    price_monthly_single_asset_index
from hg_systematic.operators import bbg_commodity_contract_fn

from hgraph.test import eval_node, EvaluationTrace

@graph
def register_services():
    register_service(default_path, trade_date_week_days)
    register_service(default_path, business_day_impl)
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
    cl_df = cl_df.rename({k: _move_back(k, 6) for k in cl_df.schema}) # Pretend these are earlier contracts
    prcs = cl_df.unpivot(cs.numeric(), index="date",variable_name="symbol", value_name="price").drop_nulls().cast({"date": date})
    register_service(default_path, price_in_dollars_static_impl, prices=prcs, round_to=2)
    register_service(default_path, monthly_rolling_info_service_impl)
    register_service(default_path, monthly_rolling_weights_impl)


def _move_back(k, delta) -> str:
    if k == "date":
        return k
    else:
        return f"CL{k[2:3]}{int(k[3:5]) - delta:2d} Comdty"


def test_single_asset_index():
    from hg_systematic.index.pricing_service import IndexResult, price_index_op

    @graph
    def g() -> TSB[IndexResult]:
        register_services()
        register_service(default_path, static_index_configuration, indices=frozendict())
        register_service(default_path, price_index_impl)
        with DebugContext("[Test]"):
            return price_monthly_single_asset_index(
                config=MonthlySingleAssetIndexConfiguration(
                    symbol="CL Index",
                    publish_holiday_calendar="BCOM",
                    rounding=8,
                    initial_level=100.0,
                    current_position=frozendict({'CLK19 Comdty': 100.0/53.30}),
                    current_position_value=frozendict({'CLK19 Comdty': 53.30}),
                    current_level=100.0,
                    start_date=date(2025, 4, 1),
                    asset="CL",
                    roll_period=(5, 10),
                    roll_schedule=("H0", "H0", "K0", "K0", "N0", "N0", "U0", "U0", "X0", "X0", "F0", "F1"),
                    trading_halt_calendar="CL NonTrading",
                    contract_fn=bbg_commodity_contract_fn
                ))

    EvaluationTrace.set_print_all_values(True)
    EvaluationTrace.set_use_logger(False)

    result = eval_node(
        g,
        __start_time__=datetime(2019, 4, 1),
        __end_time__=datetime(2019, 6, 1),
        __elide__=True,
        #__trace__=True
    )
    print('Result', result)
    assert result


def test_single_asset_index_initialised_from_nothing():
    from hg_systematic.index.pricing_service import IndexResult, price_index_op

    @graph
    def g() -> TSB[IndexResult]:
        register_services()
        register_service(default_path, static_index_configuration, indices=frozendict())
        register_service(default_path, price_index_impl)
        with DebugContext("[Test]"):
            return price_monthly_single_asset_index(
                config=MonthlySingleAssetIndexConfiguration(
                    symbol="CL Index",
                    publish_holiday_calendar="BCOM",
                    rounding=8,
                    initial_level=100.0,
                    initial_contract='CLK19 Comdty',
                    start_date=date(2019, 4, 1),
                    asset="CL",
                    roll_period=(5, 10),
                    roll_schedule=("H0", "H0", "K0", "K0", "N0", "N0", "U0", "U0", "X0", "X0", "F0", "F1"),
                    trading_halt_calendar="CL NonTrading",
                    contract_fn=bbg_commodity_contract_fn
                ))

    EvaluationTrace.set_print_all_values(True)
    EvaluationTrace.set_use_logger(False)

    result = eval_node(
        g,
        __start_time__=datetime(2019, 4, 1),
        __end_time__=datetime(2019, 6, 1),
        __elide__=True,
        #__trace__=True
    )
    print('Result', result)
    assert result