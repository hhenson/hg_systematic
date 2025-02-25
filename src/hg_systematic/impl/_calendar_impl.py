import calendar as cal
from datetime import date, timedelta, datetime
from typing import Iterable

from frozendict import frozendict as fd
from hgraph import TS, compute_node, \
    graph, service_impl, default_path, contains_, if_true, sample, TSS, TSD, map_, not_, \
    EvaluationEngineApi, generator, const, index_of

from hg_systematic.operators._calendar import Periods, business_days, business_day, calendar_for, \
    trade_date, HolidayCalendar, day_index_for

__all__ = ["business_day_impl", "trade_date_week_days", "calendar_for_static", "create_market_holidays",
           "holiday_const", "day_index_for_impl"]


@compute_node(overloads=business_days)
def business_days_impl(period: TS[Periods], calendar: HolidayCalendar, dt: TS[date],
                       _output: TS[tuple[date, ...]] = None) -> TS[tuple[date, ...]]:
    dt = dt.value
    if not period.modified and not calendar.modified and _output.valid and len(dts := _output.value) > 1:
        # Check if the date is still within the bounds, if it is then no further work required
        if dts[0] <= dt <= dts[-1]:
            return  # Don't tick any change

    sow = calendar.start_of_week.value
    eow = calendar.end_of_week.value
    holidays = calendar.holidays.value
    if holidays is None:
        holidays = frozenset()
    period = period.value
    weekends = {(eow + d) % 7 for d in range(1, (sow - eow - 1) % 7 + 1)}
    if period == Periods.Week:
        dow = dt.weekday()
        start_dt = dt - timedelta(days=(dow - sow) if dow >= sow else (dow + 7 - sow))
        count = 7 - len(weekends)
    elif period == Periods.Month:
        start_dt = dt.replace(day=1)
        count = cal.monthrange(dt.year, dt.month)[1]
    elif period == Periods.Quarter:
        start_dt = dt.replace(month=(dt.month - 1) // 3 * 3 + 1, day=1)
        count = sum(cal.monthrange(start_dt.year, start_dt.month + i)[1] for i in range(3))
    elif period == Periods.Year:
        start_dt = dt.replace(month=1, day=1)
        count = 366 if (dt.year % 4 == 0 and dt.year % 100 != 0) or (dt.year % 400 == 0) else 365
    else:
        raise ValueError(f"Unknown period {period}")
    days = tuple(dt_ for d in range(count) if
                 (dt_ := start_dt + timedelta(days=d)) not in holidays and dt_.weekday() not in weekends)
    return days


@service_impl(interfaces=(business_day,))
def business_day_impl(symbol: TSS[str], calendar_path: str = default_path, trade_date_path: str = default_path) -> TSD[
    str, TS[date]]:
    return map_(_business_day_impl, calendar_path="" if calendar_path is None else calendar_path,
                trade_date_path="" if trade_date_path is None else trade_date_path, __keys__=symbol)


@graph
def _business_day_impl(key: TS[str], calendar_path: str, trade_date_path: str) -> TS[date]:
    calendar = calendar_for(key, path=default_path if calendar_path == "" else calendar_path)
    dt = trade_date(path=default_path if trade_date_path == "" else trade_date_path)
    return sample(if_true(not_(contains_(calendar.holidays, dt))), dt)


@service_impl(interfaces=(trade_date,))
@generator
def trade_date_week_days(sow: int = 0, eow: int = 4, _api: EvaluationEngineApi = None) -> TS[date]:
    """
    Provides a trade-date generator over all weekdays. If the trade date follows a non-traditional Sat, Sun weekend,
    then supply the appropriate start and end of week.
    """
    dt = _api.start_time.date()
    end_date = _api.end_time.date()
    while dt <= end_date:
        if sow <= dt.weekday() <= eow:
            yield datetime(dt.year, dt.month, dt.day), dt
        dt += timedelta(days=1)


@service_impl(interfaces=(calendar_for,))
def calendar_for_static(symbol: TSS[str], holidays: fd[str, frozenset[date]], sow: int = 0, eow: int = 4) -> TSD[
    str, HolidayCalendar]:
    """Provide a simple stub solution to provide holiday calendars from a fixed source of holidays."""
    holidays = const(holidays, tp=TSD[str, TSS[date]])
    return map_(
        lambda hols: HolidayCalendar.from_ts(holidays=hols, start_of_week=const(sow), end_of_week=const(eow)),
        holidays,
        __keys__=symbol
    )


def create_market_holidays(countries: Iterable[str], start_date_time: datetime, end_date_time: datetime) -> frozenset[
    date]:
    """Uses the holidays package to generate out holidays for the country codes supplied"""
    import holidays
    out = set()
    years = list(range(start_date_time.year, end_date_time.year + 1))
    for ctry in countries:
        out.update(holidays.country_holidays(ctry, years=years).keys())
    return frozenset(out)


def holiday_const(holidays: frozenset[date], sow: int = 0, eow: int = 4) -> HolidayCalendar:
    """Light-weight helper for testing with holidays"""
    return const(fd(holidays=holidays, start_of_week=sow, end_of_week=eow), tp=HolidayCalendar)


@service_impl(interfaces=day_index_for)
def day_index_for_impl(symbol: TSS[str]) -> TSD[str, TS[int]]:
    """
    Provides the default implementation of the day_of_month_for service.
    """
    return map_(
        _day_of_month_for_impl, __keys__=symbol, __key_arg__="symbol"
    )


@graph
def _day_of_month_for_impl(symbol: TS[str]) -> TS[int]:
    calendar = calendar_for(symbol)
    dt = business_day(symbol)
    days_of_month = business_days(Periods.Month, calendar, dt)
    day_index = index_of(days_of_month, dt) + 1
    return day_index
