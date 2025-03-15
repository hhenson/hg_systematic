from typing import cast, Mapping

from hgraph import compute_node, cmp_, TS, TSB, CmpResult, service_impl, TSS, TSD, default_path, graph, map_, index_of, \
    switch_, len_, lift, const, cast_, if_then_else, explode

from hg_systematic.operators import MonthlyRollingRange, monthly_rolling_weights, business_day, \
    MonthlyRollingWeightRequest, calendar_for, business_days, Periods
from hg_systematic.operators._calendar import next_month
from hg_systematic.operators._rolling_rules import monthly_rolling_info, MonthlyRollingRequest, MonthlyRollingInfo, \
    rolling_schedules

__all__ = ["monthly_rolling_weights_impl", "monthly_rolling_info_service_impl", "rolling_schedules_service_impl", ]


@compute_node(overloads=cmp_)
def cmp_monthly_rolling_range(lhs: TS[int], rhs: TSB[MonthlyRollingRange], _output: TS[CmpResult] = None) \
        -> TS[CmpResult]:
    """
    Determines if the day index is in the range of the monthly rolling range.
    We only map to GT when day_index == end. When we are not in the range, we otherwise map to LT.
    """
    day_index = lhs.value
    first_day = rhs.first_day.value
    start = rhs.start.value
    end = rhs.end.value

    if day_index == end:
        out = CmpResult.GT
    elif (start < 0 and (day_index > first_day or day_index < end)) or \
            (start >= 0 and (day_index > start and day_index < end)):
        out = CmpResult.EQ
    else:
        out = CmpResult.LT
    if _output.valid and _output.value == out:
        return

    return out


@service_impl(interfaces=monthly_rolling_weights)
def monthly_rolling_weights_impl(
        request: TSS[MonthlyRollingWeightRequest],
        business_day_path: str = "",
        calendar_for_path: str = ""
) -> TSD[MonthlyRollingWeightRequest, TS[float]]:
    """
    Provides an implementation of rolling weights over a monthly rolling range.
    This will only handle requests of the form MonthlyRollingWeightRequest.

    This depends on business_day as well as calendar_for. This assumes that the calendar
    name and the business_day name are the same values.
    """
    return map_(
        _monthly_rolling_weight,
        __keys__=request, __key_arg__="request",
        business_day_path=business_day_path,
        calendar_for_path=calendar_for_path,
    )


@graph
def _monthly_rolling_weight(
        request: TS[MonthlyRollingWeightRequest],
        business_day_path: str,
        calendar_for_path: str,
) -> TS[float]:
    rolling_info = monthly_rolling_info(request)

    start_negative = rolling_info.start < 0
    roll_fraction = 1.0 / switch_(
        start_negative,
        {
            True: lambda s, e: cast(float, abs(s) + e),
            False: lambda s, e: cast(float, e - s)
        },
        rolling_info.start,
        rolling_info.end
    )
    range_ = TSB[MonthlyRollingRange].from_ts(
        first_day=rolling_info.first_day,
        start=rolling_info.start,
        end=rolling_info.end
    )
    is_rolling = rolling_info.roll_state
    weight = switch_(
        is_rolling,
        {
            CmpResult.LT: lambda d, r, f: const(1.0),
            CmpResult.EQ: lambda d, r, f: _weight(d, r, f),
            CmpResult.GT: lambda d, r, f: const(0.0),
        },
        rolling_info.day_index,
        range_,
        roll_fraction,
    )

    round_ = lift(round, inputs={"number": TS[float], "ndigits": TS[int]}, output=TS[float])
    return round_(number=weight, ndigits=request.round_to)


@graph
def _weight(day_index: TS[int], range_: TSB[MonthlyRollingRange], roll_fraction: TS[float]) -> TS[float]:
    # This is only called when we are in the range,
    # so the logic in the ``if_then_else`` clause will work correctly (else branch).
    offset = cast_(
        float,
        if_then_else(
            day_index >= range_.first_day,
            day_index - range_.first_day,
            day_index - range_.start  # This will only happen when start is negative, so we add the abs(start) (-- => +)
        )
    )
    w = 1.0 - offset * roll_fraction
    return w


@service_impl(interfaces=monthly_rolling_info)
def monthly_rolling_info_service_impl(
        request: TSS[MonthlyRollingRequest],
        business_day_path: str = "",
        calendar_for_path: str = ""
) -> TSD[MonthlyRollingRequest, TSB[MonthlyRollingInfo]]:
    return map_(
        lambda key: monthly_rolling_info_impl(key.start, key.end, key.calendar_name, business_day_path,
                                              calendar_for_path),
        __keys__=request,
    )


@graph
def monthly_rolling_info_impl(
        start: TS[int],
        end: TS[int],
        calendar_name: TS[str],
        business_day_path: str = "",
        calendar_for_path: str = ""
) -> TSB[MonthlyRollingInfo]:
    """
    Computes the rolling info for a given range and calendar name.
    """
    dt = business_day(calendar_name, path=business_day_path if business_day_path else default_path)
    calendar = calendar_for(calendar_name, path=calendar_for_path if calendar_for_path else default_path)
    days_of_month = business_days(Periods.Month, calendar, dt)
    day_index = index_of(days_of_month, dt) + 1
    start_negative = start < 0

    first_day_index = switch_(
        start_negative,
        {
            True: lambda s, dom: len_(dom) + s,
            False: lambda s, dom: s
        },
        start,
        days_of_month
    )

    range_ = TSB[MonthlyRollingRange].from_ts(
        first_day=first_day_index,
        start=start,
        end=end
    )

    is_rolling = cmp_(day_index, range_)

    roll_dt = switch_(
        start < 0,
        {
            True: lambda d, di, end: switch_(
                di > end,
                {
                    True: lambda d_: next_month(d_),
                    False: lambda d_: d_
                },
                d
            ),
            False: lambda d, di, end: d
        },
        dt,
        day_index,
        end,
    )

    y1, m1, _ = explode(roll_dt)
    m2 = (m1 % 12) + 1
    ro = m2 < m1
    y2 = if_then_else(ro, y1 + 1, y1)

    y, m, d = explode(dt)

    begin_roll = first_day_index == day_index
    end_roll = day_index == end

    return TSB[MonthlyRollingInfo].from_ts(
        first_day=first_day_index,
        start=start,
        end=end,
        days_of_month=days_of_month,
        day_index=day_index,
        dt=dt,
        day=d,
        month=m,
        year=y,
        begin_roll=begin_roll,
        end_roll=end_roll,
        roll_state=is_rolling,
        roll_out_month=m1,
        roll_out_year=y1,
        roll_in_month=m2,
        roll_in_year=y2
    )


@service_impl(interfaces=rolling_schedules)
def rolling_schedules_service_impl(
        schedules: Mapping[str, Mapping[int, tuple[int, int]]]
) -> TSD[str, TSD[int, TS[tuple[int, int]]]]:
    """Simple const implementation of rolling_schedules."""
    return const(schedules, TSD[str, TSD[int, TS[tuple[int, int]]]])
