from dataclasses import dataclass

from hgraph import TimeSeriesSchema, TS, compute_node, cmp_, TSB, CmpResult


@dataclass
class MonthlyRollingRange(TimeSeriesSchema):
    start: TS[int]
    end: TS[int]
    first_day: TS[int]  # This is the same as start when start is a positive value, and is the day index of the previous month when negative


@compute_node(overloads=cmp_)
def cmp_monthly_rolling_range(lhs: TS[int], rhs: TSB[MonthlyRollingRange]) -> TS[CmpResult]:
    """
    Determines if the day index is in the range of the monthly rolling range.
    We only map to GT when day_index == end. When we are not in the range, we otherwise map to LT.
    """
    day_index = lhs.value
    first_day = rhs.first_day.value
    start = rhs.start.value
    end = rhs.end.value

    if day_index == end:
        return CmpResult.GT
    if start < 0:
        if day_index >= first_day or day_index < end:
            return CmpResult.EQ
        else:
            return CmpResult.LT
    else:
        if day_index >= start and day_index < end:
            return CmpResult.EQ
        else:
            return CmpResult.LT


