from dataclasses import dataclass

from hgraph import TimeSeriesSchema, TS, subscription_service, default_path, CompoundScalar

__all__ = ["MonthlyRollingRange", "monthly_rolling_weights", "MonthlyRollingWeightRequest", ]


@dataclass(frozen=True)
class MonthlyRollingWeightRequest(CompoundScalar):
    """
    Specified a linear roll over the range specified.
    The start can be negatively offset to indicate the roll to this month's contract
    starts in the prior month.
    The start and end date may never overlap, there MUST be the opportunity for at
    least one value of 1.0 and one of 0.0 in any month.
    """
    start: int
    end: int
    calendar_name: str
    round_to: int


@subscription_service
def monthly_rolling_weights(request: TS[MonthlyRollingWeightRequest], path: str = default_path) -> TS[float]:
    """
    Produces a stream of rolling weights over the given calendars business days.
    This will only tick a value if the result is modified, i.e. it does not tick
    each time a date changes, but only when the result is different.
    """


@dataclass
class MonthlyRollingRange(TimeSeriesSchema):
    start: TS[int]
    end: TS[int]
    first_day: TS[int]  # This is the same as start when start is a positive value, and is the day index of the
    # previous month when negative


