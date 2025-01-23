from datetime import date
from enum import Enum

from hgraph import TimeSeriesSchema, TSS, subscription_service, TS, default_path, TSB, operator


__all__ = ["CalendarSchema", "calendar_for", "Periods", "business_days"]


class CalendarSchema(TimeSeriesSchema):
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


@subscription_service
def calendar_for(symbol: TS[str], path: str = default_path) -> TSB[CalendarSchema]:
    """
    The calendar service for a given symbol.
    """


class Periods(Enum):
    Week = 1
    Month = 2
    Quarter = 3
    Year = 4


@operator
def business_days(period: TS[Periods], calendar: TSB[CalendarSchema], dt: TS[date] = None) -> TS[tuple[date, ...]]:
    """
    Identifies the business days for the given period, using the given calendar.
    This will be for the period containing the current engine clock or (if provided) the
    dt provided.
    """