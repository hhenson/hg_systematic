from datetime import date
from typing import cast, Mapping, Any

from hgraph import compute_node, cmp_, TS, TSB, CmpResult, service_impl, TSS, TSD, default_path, graph, map_, index_of, \
    switch_, len_, lift, const, cast_, if_then_else, dedup, TSL, Size, explode, TimeSeriesList, TimeSeriesValueInput

from hg_systematic.operators import MonthlyRollingRange, monthly_rolling_weights, business_day, \
    MonthlyRollingWeightRequest, calendar_for, business_days, Periods, rolling_contracts_for

__all__ = ["monthly_rolling_weights_impl"]


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
    symbol = request.calendar_name
    dt = business_day(symbol, path=business_day_path if business_day_path else default_path)
    calendar = calendar_for(symbol, path=calendar_for_path if calendar_for_path else default_path)
    days_of_month = business_days(Periods.Month, calendar, dt)
    day_index = index_of(days_of_month, dt) + 1

    start = request.start
    end = request.end
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
    round_ = lift(round, inputs={"number": TS[float], "ndigits": TS[int]}, output=TS[float])
    roll_fraction = 1.0 / switch_(
        start_negative,
        {
            True: lambda s, e: cast(float, abs(s) + e),
            False: lambda s, e: cast(float, e-s)
        },
        start,
        end
    )
    range_ = TSB[MonthlyRollingRange].from_ts(
        first_day=first_day_index,
        start=start,
        end=end
    )
    is_rolling = cmp_(day_index, range_)
    weight = switch_(
        is_rolling,
        {
            CmpResult.LT: lambda d, r, f: const(1.0),
            CmpResult.EQ: lambda d, r, f: _weight(d, r, f),
            CmpResult.GT: lambda d, r, f: const(0.0),
        },
        day_index,
        range_,
        roll_fraction,
    )
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


@service_impl(interfaces=rolling_contracts_for)
def rolling_contracts_for_impl(
        symbol: TSS[str],
        roll_schedule: Mapping[str, Mapping[int, tuple[int, int]]],
        format_str: Mapping[str, str],
        year_scale: Mapping[str, int],
        dt_symbol: Mapping[str, str]
) -> TSD[str, TSL[TS[str], Size[2]]]:

    roll_schedule = const(roll_schedule, TSD[str, TSD[int, TS[tuple[int, int]]]])
    format_str = const(format_str, TSD[str, TS[str]])
    year_scale = const(year_scale, TSD[str, TS[int]])
    dt_symbol = const(dt_symbol, TSD[str, TS[str]])
    dts = map_(lambda dt: business_day(dt), dt_symbol)
    return map_(_roll_contracts, dts, roll_schedule, format_str, year_scale)


@graph
def _roll_contracts(
        dt: TS[date],
        roll_schedule: TSD[int, TS[tuple[int, int]]],
        format_str: TS[str],
        year_scale: TS[int]
) -> TSL[TS[str], Size[2]]:
    from hg_systematic.operators._rolling_rules import _create_contract
    y1, m1, _ = explode(dt)
    m2 = (m1 % 12) + 1
    ro = m2 < m1
    y2 = if_then_else(ro, y1 + 1, y1)
    return TSL[TS[str], Size[2]].from_ts(
        _create_contract(m1, y1, roll_schedule, format_str, year_scale),
        _create_contract(m2, y2, roll_schedule, format_str, year_scale)
    )
