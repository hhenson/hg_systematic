from datetime import date, timedelta

from hgraph import TS, TSB, compute_node, \
    last_modified_date

from hg_systematic.operators._calendar import Periods, CalendarSchema, business_days

__all__ = tuple()


@compute_node(overloads=business_days)
def business_days_impl(period: TS[Periods], calendar: TSB[CalendarSchema], dt: TS[date] = None) -> TS[tuple[date, ...]]:
    """
    Identifies the business days for the given period, using the given calendar.
    This will be for the period containing the current engine clock or (if provided) the
    dt provided.
    """
    if dt is None:
        dt = last_modified_date()
    return _business_days(period, calendar, dt)

@compute_node
def _business_days(period: TS[Periods], calendar: TSB[CalendarSchema], dt: TS[date]) -> TS[tuple[date, ...]]:
    dt = dt.value
    sow = calendar.start_of_week.value
    eow = calendar.end_of_week.value
    holidays = calendar.holidays.value
    period = period.value
    weekends = {(sow + d) % 7 for d in range(1, sow-eow%7+1)}
    if period == Periods.Week:
        dow = dt.weekday()
        start_dt = dt - timedelta(days=7 + (sow-dow) % 7)
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
    days = tuple(dt_ for d in range(count) if (dt_ := start_dt + timedelta(days=d)) not in holidays and dt_.weekday() not in weekends)
    return days

