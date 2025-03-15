from dataclasses import dataclass
from datetime import date
from typing import Callable

from hg_oap.instruments.future import month_code
from hgraph import TimeSeriesSchema, TS, subscription_service, default_path, CompoundScalar, graph, TSD, TSB, TSL, Size, \
    CmpResult, reference_service, format_, lift, compute_node, apply

__all__ = ["MonthlyRollingRange", "monthly_rolling_weights", "MonthlyRollingRequest", "MonthlyRollingWeightRequest",
           "monthly_rolling_info", "MonthlyRollingInfo", "rolling_contracts", "bbg_commodity_contract_fn",
           "rolling_schedules"]


@dataclass(frozen=True)
class MonthlyRollingRequest(CompoundScalar):
    """
    Defines the rolling request parameters.
    This is the base information needed to determine a monthly roll schedule.

    :param start: The start of the roll range, this can be negative.
    :param end: The end of the roll range. This may not be negative.
    :param calendar_name: The holiday calendar to use when determining business dates.
    """
    start: int
    end: int
    calendar_name: str


@dataclass(frozen=True)
class MonthlyRollingWeightRequest(MonthlyRollingRequest):
    """
    Specified a linear roll over the range specified.
    The start can be negatively offset to indicate the roll to this month's contract
    starts in the prior month.
    The start and end date may never overlap, there MUST be the opportunity for at
    least one value of 1.0 and one of 0.0 in any month.
    """
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


@dataclass
class MonthlyRollingInfo(MonthlyRollingRange):
    days_of_month: TS[tuple[date]]  # The dates within the month
    day_index: TS[int]  # The index within the days_of_month
    dt: TS[date]  # The date that the information represents
    # --- exploded values of date
    day: TS[int]
    month: TS[int]
    year: TS[int]
    # ---
    begin_roll: TS[bool]
    end_roll: TS[bool]
    roll_state: TS[CmpResult]  # LT before roll, EQ in roll, GT After roll
    roll_out_month: TS[int]
    roll_out_year: TS[int]
    roll_in_month: TS[int]
    roll_in_year: TS[int]


@subscription_service
def monthly_rolling_info(request: TS[MonthlyRollingRequest], path: str = default_path) -> TSB[MonthlyRollingInfo]:
    """
    The raw rolling information that can used to generate out weights or contracts.
    """


@graph
def rolling_contracts(
        roll_info: TSB[MonthlyRollingInfo],
        roll_schedule: TSD[int, TS[tuple[int, int]]],
        asset: TS[str],
        contract_fn: TS[Callable[[TS[str], TS[int], TS[int]], TS[str]]],
) -> TSL[TS[str], Size[2]]:
    """
    The contracts for the given roll_info and contract_fn.

    The ``contract_fn`` converts the month and year into a string for the name.
    For example:

    ::

        @graph
        def bbg_commodity_contract_fn(asset: TS[str], month: TS[str], year: TS[int]) -> TS[str]:
            y = year % 100
            return format_(
                "{asset}{month}{year} Comdty",
                month=lift(month_code, inputs={"d": TS[int]})(month),
                year=y
            )

    """
    m1 = roll_info.roll_out_month
    y1 = roll_info.roll_out_year
    m2 = roll_info.roll_in_month
    y2 = roll_info.roll_in_year

    c1_m = _create_contract(
        month=m1,
        year=y1,
        schedule=roll_schedule,
        asset=asset,
        contract_fn=contract_fn
    )

    c2_m = _create_contract(
        month=m2,
        year=y2,
        schedule=roll_schedule,
        asset=asset,
        contract_fn=contract_fn
    )

    return TSL.from_ts(c1_m, c2_m)


@graph
def _create_contract(
        month: TS[int],
        year: TS[int],
        schedule: TSD[int, TS[tuple[int, int]]],
        asset: TS[str],
        contract_fn: TS[Callable[[TS[str], TS[int], TS[int]], TS[str]]]) -> TS[str]:
    s = schedule[month]
    m = s[0]
    y = year + s[1]
    return apply[TS[str]](contract_fn, asset, m, y)


@reference_service
def rolling_schedules(path: str = default_path) -> TSD[str, TSD[int, TS[tuple[int, int]]]]:
    """
    The rolling contracts for the symbol.
    """


def bbg_commodity_contract_fn(asset: str, month: int, year: int) -> str:
    return f"{asset}{month_code(month)}{year % 100:02d} Comdty"
