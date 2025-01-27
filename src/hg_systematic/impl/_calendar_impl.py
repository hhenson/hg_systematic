from datetime import date, timedelta, datetime

from frozendict import frozendict as fd
from hgraph import TS, TSB, compute_node, \
    last_modified_date, graph, schedule, service_impl, default_path, contains_, if_true, sample, TSS, TSD, map_, not_, \
    EvaluationEngineApi, generator, const

from hg_systematic.operators._calendar import Periods, HolidayCalendarSchema, business_days, business_day, calendar_for, \
    trade_date, HolidayCalendar

__all__ = ["business_day_impl", "trade_date_week_days", "calendar_for_static"]


@graph(overloads=business_days)
def business_days_impl(period: TS[Periods], calendar: HolidayCalendar, dt: TS[date] = None) -> TS[tuple[date, ...]]:
    """
    Identifies the business days for the given period, using the given calendar.
    This will be for the period containing the current engine clock or (if provided) the
    dt provided.
    """
    if dt is None:
        dt = last_modified_date(schedule(delay=timedelta(days=1), initial_delay=False))
    return _business_days(period, calendar, dt)


@compute_node
def _business_days(period: TS[Periods], calendar: HolidayCalendar, dt: TS[date],
                   _output: TS[tuple[date, ...]] = None) -> TS[tuple[date, ...]]:
    dt = dt.value
    if not period.modified and not calendar.modified and _output.valid and len(dts := _output.value) > 1:
        # Check if the date is still within the bounds, if it is then no further work required
        if dts[0] <= dt <= dts[-1]:
            return  # Don't tick any change

    sow = calendar.start_of_week.value
    eow = calendar.end_of_week.value
    holidays = calendar.holidays.value
    period = period.value
    weekends = {(sow + d) % 7 for d in range(1, (sow - eow) % 7 + 1)}
    if period == Periods.Week:
        dow = dt.weekday()
        start_dt = dt - timedelta(days=7 + (sow - dow) % 7)
        count = (eow - sow) % 7
    elif period == Periods.Month:
        start_dt = dt.replace(day=1)
        count = dt.monthrange(dt.year, dt.month)[1]
    elif period == Periods.Quarter:
        start_dt = dt.replace(month=(dt.month - 1) // 3 * 3 + 1, day=1)
        count = sum(dt.monthrange(start_dt.year, start_dt.month + i)[1] for i in range(3))
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
    dt = trade_date(path=default_path if trade_date_path=="" else trade_date_path)
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
