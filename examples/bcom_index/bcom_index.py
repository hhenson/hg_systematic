"""
Computations of settlement price:
Prior to the Roll Period
On Business Day 1 of the month, the Index is calculated as follows:

BCOM_S= BCOM_PS * WAV1_S/WAV2_PS # Note that WAV1_S == WAV2_PS since we switch our view of rolls at SOM

On Business Days 2 through 5 of the month, BCOM is calculated as follows:

BCOM_S = BCOM_PS * WAV1_S / WAV1_PS

During the Roll Period
On each day of the Roll Period, the dependence of BCOM is shifted, at the rate of 20% per day, from WAV1
to WAV2 as follows:

Day 1 of Roll Period (Business Day 6 of Month):
BCOM_S = BCOM_PS*(WAV1_S*.80 + WAV2_S*.20)/(WAV1_PS *.80+ WAV2_PS*.20)

Day 2 of Roll Period (Business Day 7 of Month):
BCOM_S = BCOM_PS*(WAV1_S*.60 + WAV2_S*.40)/(WAV1_PS * .60+ WAV2_PS*.40)

Day 3 of Roll Period (Business Day 8 of Month):
BCOM_S = BCOM_PS*(WAV1_S*.40 + WAV2_S*.60)/(WAV1_PS * .40+ WAV2_PS*.60)

Day 4 of Roll Period (Business Day 9 of Month):
BCOM_S = BCOM_PS*(WAV1_S*.20 + WAV2_S*.80)/(WAV1_PS *.20+ WAV2_PS*.80)

Day 5 of Roll Period (Business Day 10 of Month):
BCOM_S = BCOM_PS *(WAV2_S / WAV2_PS)

(3) After the Roll Period
For the remainder of the month, the calculation of BCOM will be
BCOM_S = BCOM _PS *(WAV2_S / WAV2_PS)


(rounded to 8 decimal places)

assets =
F_S = map_(lambda x: price_in_dollars(x), symbol)
WAV1 = sum(contract[0]*CIM[0])

CIM1 = CIM2 on day after last day of Roll Period in January
"""
from datetime import date
from enum import Enum

from hgraph import graph, TS, const, map_, Size, pass_through, reduce, add_, TSD, TSL, feedback, lag, index_of, \
    if_then_else, month_of_year, default, passive, component, TSB, ts_schema, union, div_, DivideByZero, SIZE, switch_, \
    year, format_
from pygments.unistring import combine

from hg_systematic.operators import index_assets, index_composition, calendar_for, index_rolling_contracts, \
    business_day, index_rolling_weights, price_in_dollars, SETTLEMENT_PRICE, business_days, Periods, INDEX_WEIGHTS, \
    HolidayCalendar, ContractMatchState, month_code
from hg_systematic.operators._index import index_rolling_schedule


class BcomCalcState(Enum):
    CIM_SHIFT = 1
    ON_BUSINESS_DAY_1 = 2
    ON_BUSINESS_DAY_2_THROUGH_5 = 3
    DURING_ROLL = 4
    AFTER_ROLL = 5


@graph
def bcom_settlement(bcom_prev_settlement: float = 100.0) -> TS[float]:
    """
    Determine the settlement price for a BCOM contract as of now.
    This requires an initialising previous settlement as the definition of the index is WRT the
    previous price. By default, we initialise with 100.0, but if we are starting a simulation from
    a point-in-time where a previous price can be obtained, this can be used as an input into the computation.
    :return: The price in USD
    """
    symbol = const("BCOM Index")
    assets = index_assets(symbol)
    # These weights are expected to be the CIMS in this case.
    asset_weights = index_composition(symbol)
    calendar = calendar_for(symbol)
    dt = business_day(symbol)
    # Note this requires us obtaining the asset_weights for the previous year if we are pricing during jan!
    month = month_of_year(dt)
    cim1 = if_then_else(month == 1, lag(asset_weights), asset_weights)
    cim2 = asset_weights

    bcom_ps = feedback(TS[float], bcom_prev_settlement)
    wav1_ps = feedback(TS[float], 0.0)
    wav2_ps = feedback(TS[float], 0.0)
    out = calc_bcom_settlement(bcom_ps, wav1_ps=wav1_ps, wav2_ps=wav2_ps, cim1=cim1, cim2=cim2, calendar=calendar, dt=dt )
    bcom_ps(out.bcom_s)
    wav1_ps(out.wav1_s)
    wav2_ps(out.wav2_s)
    return out.bcom_s


@component
def calc_bcom_settlement(
        bcom_ps: TS[float],
        wav1_ps: TS[float],
        wav2_ps: TS[float],
        cim1: INDEX_WEIGHTS,
        cim2: INDEX_WEIGHTS,
        calendar: HolidayCalendar,
        dt: TS[date],
) -> TSB[
    ts_schema(bcom_s=TS[float], wav1_s=TS[float], wav2_s=TS[float])
]:


    # The computation requires at least one cycle of computation prepare some of the artifacts.

    rolling_rule = "BCOM"
    rolling_size = Size[2]

    assets = union(cim1.key_set, cim2.key_set)
    rolling_contracts = map_(
        lambda key, dt_: index_rolling_contracts(rolling_rule, key, dt_, sz=rolling_size), dt, __keys__=assets)
    rolling_weights = map_(
        lambda key, dt_, calendar_: index_rolling_weights(rolling_rule, calendar_, dt_, sz=rolling_size), dt,
        pass_through(calendar), __keys__=assets)

    days_of_month = business_days(Periods.Month, calendar, dt)
    day_index = index_of(days_of_month, dt) + 1

    wav1_s = compute_wav(0, rolling_contracts, cim1)
    wav2_s = compute_wav(1, rolling_contracts, cim2)
    wav1_ps = if_then_else(day_index != 1, wav1_ps, wav2_ps)  # For SOM we need to use last months wav2 for wav1

    # The feedback (bcom_ps()) needs to be passive and not force a re-computation of the value.
    bcom_s = passive(bcom_ps()) * div_((wav1_s * rolling_weights[0] + wav2_s * rolling_weights[1]), (
            wav1_ps * rolling_weights[0] + wav2_ps * rolling_weights[1]), divide_by_zero = DivideByZero.NONE)
    # We need to make sure we sample at an appropriate time to get the price, i.e. once all the raw closing prices have
    # been processed.

    return combine(bcom_s=bcom_s, wav1_s=wav1_s, wav2_s=wav2_s)


@graph
def compute_wav(
        offset: int,
        rolling_contracts: TSD[str, TSL[TS[str], Size[2]]],
        rolling_weights: TSD[str, TS[float]]
) -> TS[float]:
    """Compute the weighted average value of the rolling contracts."""
    return reduce(
        lambda x, y: x + y,
        map_(
            lambda contract, weight: price_in_dollars(contract[offset], path=SETTLEMENT_PRICE) * weight,
            rolling_contracts,
            rolling_weights,
        )
    )


@graph(overloads=index_rolling_weights, requires=lambda m, s: m[SIZE].py_type == Size[2] and s["rule"] == "BCOM")
def _bcom_rolling_weights(
        rule: str,
        calendar: HolidayCalendar,
        dt: TS[date],
        sz: SIZE = Size[2]
) -> TSL[TS[float], Size[2]]:
    days_of_month = business_days(Periods.Month, calendar, dt)
    day_index = index_of(days_of_month, dt) + 1
    rule_state = if_then_else(
        day_index < 6,
        ContractMatchState.LEFT_ONLY,
        if_then_else(
            day_index > 10,
            ContractMatchState.RIGHT_ONLY,
            ContractMatchState.OTHERWISE
        )
    )

    return switch_(
        {
            ContractMatchState.LEFT_ONLY: lambda ndx: const((1.0, 0.0), TSL[TS[float], Size[2]]),
            ContractMatchState.OTHERWISE: lambda ndx: TSL[TS[float], Size[2]].from_ts((10 - ndx) * .2, (ndx - 5) * .2),
            ContractMatchState.RIGHT_ONLY: lambda ndx: const((0.0, 1.0), TSL[TS[float], Size[2]]),
        },
        rule_state,
        day_index
    )


@graph(overloads=index_rolling_contracts, requires=lambda m, s: m[SIZE].py_type == Size[2] and s["rule"] == "BCOM")
def _bcom_rolling_contracts(
        rule: str,
        symbol: TS[str],
        dt: TS[date],
        sz: SIZE = Size[2]
) -> TSL[TS[str], Size[2]]:
    roll_schedule = index_rolling_schedule("BCOM Index")[symbol]
    m = month_of_year(dt)
    y = year(dt)
    # The roll from month and year
    r1_m = roll_schedule[m][0]
    r1_y = y + roll_schedule[m][1]
    # The next month and year for given date
    m1 = (m % 12) + 1
    y1 = if_then_else(m1 == 0, y + 1, y)
    # The roll to month and year
    r2_m = roll_schedule[m1][0]
    r2_y = y1 + roll_schedule[m1][1]
    # Now we make a weak assumption that the contract format takes the form of:
    # <key><month in letter><2 digit year> Comdty
    return TSL[TS[str], Size[2]].from_ts(
        format_("{}{}{:02} Comdty", symbol, month_code(r1_m), r1_y % 100),
        format_("{}{}{:02} Comdty", symbol, month_code(r2_m), r2_y % 100)
    )
