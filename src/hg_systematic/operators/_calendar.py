from datetime import date
from enum import Enum

from hgraph import TimeSeriesSchema, TSS, subscription_service, TS, default_path, TSB, operator, reference_service, \
    compute_node, contains_, graph, TIME_SERIES_TYPE, last_modified_date, sample, if_true, not_

__all__ = ["HolidayCalendarSchema", "calendar_for", "Periods", "business_days", "business_day", "HolidayCalendar",
           "filter_by_calendar", "day_index_for"]


class HolidayCalendarSchema(TimeSeriesSchema):
    """
    The typical schema here is to track holidays and the start and end of the week. With the days between
    start of week and end of week being weekends or non-working days.
    If this is not appropriate the logic should also be able to cope with no-weekends specified (i.e. sow and eow being
    monday and sunday respectively) and any actual weekends being encoded in the holidays. This supports a more flexible
    specification that supports weekends shifting (as is the case with the UAE recently).
    Alternatively the calendar could be adjusted using the point-in-time of the engine clock.
    This is an implementation choice and needs to be clearly specified so the user can make an appropriate decision.
    """
    holidays: TSS[date]
    start_of_week: TS[int]
    end_of_week: TS[int]


HolidayCalendar = TSB[HolidayCalendarSchema]


@subscription_service
def calendar_for(symbol: TS[str], path: str = default_path) -> HolidayCalendar:
    """
    The calendar service for a given symbol.
    """


class Periods(Enum):
    Week = 1
    Month = 2
    Quarter = 3
    Year = 4


@operator
def business_days(period: TS[Periods], calendar: HolidayCalendar, dt: TS[date]) -> TS[tuple[date, ...]]:
    """
    Identifies the business days for the given period, using the given calendar.
    This will be for the period containing the current engine clock or (if provided) the
    dt provided.
    """


@reference_service
def trade_date(path: str = default_path) -> TS[date]:
    """
    The current trade date for this process. The trade date is wired in to the outer graph and defines the sessions
    trading date. Whilst it is possible to have multiple trading dates simultaneously, this not the general case.
    When multiple trade dates are possible, the path would disambiguate the trading dates. Note that a trading date
    does not imply that a contract being considered for trading would be tradable, just that if it were to trade we would
    consider the trade date to be the date of record for the trade.
    """


@subscription_service
def business_day(symbol: TS[str], path: str = default_path) -> TS[date]:
    """
    This will make use of the trade_date to track if the current trade_date is also a business day for the instrument.
    This is slightly different to just walking the business days of a calendar, as if we are not trading and the instrument
    is tradeable, we will still not produce a date.
    """


@compute_node(overloads=contains_)
def _contains_dt_in_calendar(ts: HolidayCalendar, item: TS[date]) -> TS[bool]:
    """
    Determines if the date is within the holiday calendar, for us that means if we deem it a non-working day then
    the date is within the calendar.
    """
    dt = item.value
    dow = dt.weekday()
    sow = ts.start_of_week.value
    eow = ts.end_of_week.value
    if eow < sow:
        if eow < dow < sow:
            return True  # We are in a weekend
    else:
        if dow < sow or eow < dow:
            return True  # The other perspective for weekend
    return dt in ts.holidays.value


@graph
def filter_by_calendar(ts: TIME_SERIES_TYPE, holidays: HolidayCalendar) -> TIME_SERIES_TYPE:
    """Restrict values to be published only during working days"""
    dt = last_modified_date(ts)
    return sample(if_true(not_(contains_(holidays, dt))), ts)


@compute_node
def next_month(dt: TS[date], _output: TS[date] = None) -> TS[date]:
    """
    Returns the first date of the next month
    """
    dt = dt.value
    y = dt.year
    m = dt.month
    if m == 12:
        out = date(y + 1, 1, 1)
    else:
        out = date(y, m + 1, 1)
    if not _output.valid or out != _output.value:
        return out


@subscription_service
def day_index_for(symbol: TS[str], path: str = default_path) -> TS[int]:
    """
    Determines the day of the month for a given symbol,
    the symbol is the calendar_for symbol used to extract the holiday calendar.

    This depends on calendar_for service.
    """

