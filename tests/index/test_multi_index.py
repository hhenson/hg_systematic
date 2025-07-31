from dataclasses import dataclass
from datetime import date, datetime
from importlib import resources as pkg_resources

import polars as pl
import polars.selectors as cs
from frozendict import frozendict
from hgraph import graph, register_service, default_path, TSB, DebugContext, TS, const, map_
from hgraph.test import eval_node, EvaluationTrace

from hg_systematic.impl import trade_date_week_days, calendar_for_static, create_market_holidays, \
    price_in_dollars_static_impl, monthly_rolling_info_service_impl, monthly_rolling_weights_impl, business_day_impl
from hg_systematic.index.configuration_service import static_index_configuration
from hg_systematic.index.multi_index import AnnualMultiIndexConfiguration, multi_index_monthly_rolling_index

from hg_systematic.index.single_asset_index import MonthlySingleAssetIndexConfiguration, set_single_index_debug_on
from hg_systematic.operators import bbg_commodity_contract_fn


@dataclass(frozen=True)
class MyMultiIndexConfiguration(AnnualMultiIndexConfiguration):
    ...

INDICES = {
    "My Index": MyMultiIndexConfiguration(
        symbol="My Index",
        publish_holiday_calendar="BCOM",
        rounding=8,
        initial_level=100.0,
        current_position=frozendict({'CL Index': 100.0 / 2.0, 'LA Index': 100.0 / 2.0}),
        current_position_value=frozendict({'CL Index': 100.0, 'LA Index': 100.0}),
        current_level=100.0,
        start_date=date(2018, 4, 1),
        indices=("CL Index", "LA Index"),
        roll_period=(1, 4)
    ),
    "CL Index": MonthlySingleAssetIndexConfiguration(
        symbol="CL Index",
        publish_holiday_calendar="BCOM",
        rounding=8,
        initial_level=100.0,
        current_position=frozendict({'CLK19 Comdty': 100.0 / 53.30}),
        current_position_value=frozendict({'CLK19 Comdty': 53.30}),
        current_level=100.0,
        start_date=date(2018, 4, 1),
        asset="CL",
        roll_period=(5, 10),
        roll_schedule=("H0", "H0", "K0", "K0", "N0", "N0", "U0", "U0", "X0", "X0", "F0", "F1"),
        trading_halt_calendar="CL NonTrading",
        contract_fn=bbg_commodity_contract_fn
    ),
    "LA Index": MonthlySingleAssetIndexConfiguration(
        symbol="LA Index",
        publish_holiday_calendar="BCOM",
        rounding=8,
        initial_level=100.0,
        current_position=frozendict({'LAK19 Comdty': 100.0 / (53.30 * 1.7)}),
        current_position_value=frozendict({'LAK19 Comdty': 53.30 * 1.7}),
        current_level=100.0,
        start_date=date(2018, 4, 1),
        asset="LA",
        roll_period=(5, 10),
        roll_schedule=("H0", "H0", "K0", "K0", "N0", "N0", "U0", "U0", "X0", "X0", "F0", "F1"),
        trading_halt_calendar="LA NonTrading",
        contract_fn=bbg_commodity_contract_fn
    ),
}


@graph
def register_services():
    register_service(default_path, trade_date_week_days)
    register_service(default_path, business_day_impl)
    register_service(
        default_path, calendar_for_static,
        holidays=frozendict({
            "BCOM": create_market_holidays(["US"], date(2018, 1, 1), date(2030, 1, 1)),
            "CL NonTrading": create_market_holidays(["US"], date(2018, 1, 1), date(2030, 1, 1)),
            "LA NonTrading": create_market_holidays(["UK", "US"], date(2018, 1, 1), date(2030, 1, 1)),
        }),
    )
    import tests.index
    with pkg_resources.path(tests.index, "CL.parquet") as file:
        raw = pl.read_parquet(file)
    cl_df = raw.rename({k: _move_back(k, 6) for k in raw.schema})  # Pretend these are earlier contracts
    la_df = raw.rename({k: _move_back(k, 6, "LA") for k in raw.schema}).with_columns(
        pl.selectors.numeric() * 1.7)  # Pretend these are earlier contracts and LA
    prcs = pl.concat([
        cl_df.unpivot(cs.numeric(), index="date", variable_name="symbol", value_name="price").drop_nulls().cast(
            {"date": date}),
        la_df.unpivot(cs.numeric(), index="date", variable_name="symbol", value_name="price").drop_nulls().cast(
            {"date": date}),
    ],
        how="vertical",
        rechunk=True,
    ).sort("date", "symbol")
    register_service(default_path, price_in_dollars_static_impl, prices=prcs, round_to=2)
    register_service(default_path, monthly_rolling_info_service_impl)
    register_service(default_path, monthly_rolling_weights_impl)
    register_service(default_path, static_index_configuration, indices=INDICES)


def _move_back(k, delta, symbol="CL") -> str:
    if k == "date":
        return k
    else:
        return f"{symbol}{k[2:3]}{int(k[3:5]) - delta:2d} Comdty"


def test_multi_index():
    from hg_systematic.index.pricing_service import IndexResult, price_index_op, price_index_impl, \
        price_index
    @graph(overloads=price_index_op)
    def price_index_op_my(config: TS[MyMultiIndexConfiguration]) -> TSB[IndexResult]:
        with DebugContext(prefix="[test]"):
            return multi_index_monthly_rolling_index(
                config=config,
                weights_fn=lambda cfg, levels: map_(lambda key: const(0.5), __keys__=levels.key_set)
            )

    @graph
    def g(symbol: TS[str]) -> TSB[IndexResult]:
        register_services()
        register_service(default_path, price_index_impl)
        with DebugContext("[Test]"):
            return price_index(symbol)

    EvaluationTrace.set_print_all_values(True)
    EvaluationTrace.set_use_logger(False)

    set_single_index_debug_on()

    result = eval_node(
        g,
        ['My Index'],
        __start_time__=datetime(2019, 4, 1),
        __end_time__=datetime(2019, 6, 1),
        __elide__=True,
        #__trace__=True
    )
    print('Result', result)
    assert result
