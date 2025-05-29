"""
The BCOM Index is defined in https://assets.bbhub.io/professional/sites/10/BCOM-Methodology.pdf
This model here shows a basic implementation of these rules, not including any logic to handle
trading disruption events, etc.
"""
from datetime import date, datetime
from typing import Mapping

import polars as pl
from frozendict import frozendict as fd
from hg_oap.instruments.future import month_from_code
from hgraph import graph, TS, const, TSD, index_of, if_then_else, switch_, lift, CmpResult, explode, register_service, \
    default_path, cast_, Frame, map_
from importlib_resources import files, as_file

from hg_systematic.impl import calendar_for_static, StaticPriceSchema
from hg_systematic.impl._calendar_impl import create_market_holidays
from hg_systematic.operators import index_composition, index_rolling_contracts, \
    index_rolling_weight, business_days, Periods, \
    HolidayCalendar, symbol_is, INDEX_ROLL_FLOAT, INDEX_ROLL_STR
from hg_systematic.operators._rolling_rules import monthly_rolling_info, MonthlyRollingRequest, \
    futures_rolling_contracts, bbg_commodity_contract_fn


@graph(overloads=index_rolling_weight, requires=symbol_is("BCOM Index"))
def index_rolling_weights_bcom(symbol: str, dt: TS[date], calendar: HolidayCalendar) -> TS[float]:
    days_of_month = business_days(Periods.Month, calendar, dt)
    day_index = index_of(days_of_month, dt) + 1
    is_rolling = if_then_else(day_index < 6, CmpResult.LT, if_then_else(day_index > 9, CmpResult.GT, CmpResult.EQ))
    return switch_(
        is_rolling,
        {
            CmpResult.LT: lambda d: const(1.0),
            CmpResult.EQ: lambda d: lift(lambda x: round(x, 1), inputs={"x": TS[float]}, output=TS[float])(
                (10 - d) * .2),
            CmpResult.GT: lambda d: const(0.0),
        },
        cast_(float, day_index),
    )


def get_cims_for_year(year: int) -> Mapping[str, float]:
    """Load the cims from the csv file"""
    import examples.bcom_index
    source = files(examples.bcom_index).joinpath("bcom_cims.csv")
    with as_file(source) as resource_path:
        df = pl.read_csv(resource_path)
    year_column = str(year)
    return fd(df.select("Commodity", year_column).iter_rows())


@graph(overloads=index_composition, requires=symbol_is("BCOM Index"))
def index_composition_bcom(symbol: str, dt: TS, calendar: HolidayCalendar) -> INDEX_ROLL_FLOAT:
    y, m, _ = explode(dt)
    y_prev = y - 1
    get_cims = lift(get_cims_for_year, output=TSD[str, TS[float]])
    cim2 = get_cims(y)
    prev = get_cims(y_prev)
    cim1 = if_then_else(m == 1, prev, cim2)
    return INDEX_ROLL_FLOAT.from_ts(first=cim1, second=cim2)


def get_bcom_roll_schedule() -> Mapping[str, Mapping[int, tuple[int, int]]]:
    import examples.bcom_index
    source = files(examples.bcom_index).joinpath("bcom_roll_schedule.csv")
    with as_file(source) as resource_path:
        df = pl.read_csv(resource_path)
    return fd((k[0], fd(
        (month_from_code(k_),
         (month_from_code((i := v_.item())[0]), int(i[1]))) for k_, v_ in v.to_dict().items())) for
              k, v in df.partition_by("Commodity", include_key=False, as_dict=True).items())


@graph(overloads=index_rolling_contracts, requires=symbol_is("BCOM Index"))
def index_rolling_contracts_bcom(symbol: str, dt: TS, calendar: HolidayCalendar) -> INDEX_ROLL_STR:
    rs = get_bcom_roll_schedule()
    roll_schedule = const(rs, TSD[str, TSD[int, TS[tuple[int, int]]]])
    fmt_str = const(fd({k: f"{k}{{month}}{{year:02d}} Comdty" for k in rs.keys()}), TSD[str, TS[str]])
    year_scale = const(fd({k: 100 for k in rs.keys()}), TSD[str, TS[int]])
    rolling_info = monthly_rolling_info(
        request=MonthlyRollingRequest(5, 10, "BCOM")
    )
    contracts = map_(
        lambda asset, ri, rs, c_fn: futures_rolling_contracts(ri, rs, asset, c_fn),
        rolling_info,
        roll_schedule,
        bbg_commodity_contract_fn,
        __keys__=roll_schedule.key_set,
        __key_arg__="asset",
    )
    first = map_(lambda c: c[0], contracts)
    second = map_(lambda c: c[1], contracts)
    return INDEX_ROLL_STR.from_ts(first=first, second=second)


def create_bcom_holidays() -> frozenset[date]:
    return create_market_holidays(["US", "GB"], datetime(1996, 1, 1), datetime(2035, 1, 1))


def register_bcom_static_calendar():
    """
    Use an approximation of the holidays using a US and GB calendar. For now load from 1996 through 2035.
    """
    register_service(default_path, calendar_for_static, holidays=fd(
        {"BCOM Index": create_bcom_holidays()}))


def load_sample_prices() -> Frame[StaticPriceSchema]:
    import examples.bcom_index
    source = files(examples.bcom_index).joinpath("bcom_prices.csv")
    with as_file(source) as resource_path:
        df = pl.read_csv(resource_path)
    return df.melt("Commodity", variable_name="date", value_name="price").cast({"date": date}).rename(
        {"Commodity": "symbol"}).select("date", "symbol", "price").sort("date")
