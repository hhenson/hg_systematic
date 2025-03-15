from dataclasses import dataclass
from operator import not_, or_
from typing import Callable

from frozendict import frozendict
from hgraph import graph, TS, combine, map_, TSB, TimeSeriesSchema, Size, TSL, convert, TSS, feedback, \
    const, union, no_key, reduce, if_then_else, sample, passive, switch_, CmpResult, len_, all_, contains_, \
    default

from hg_systematic.index.configuration import SingleAssetIndexConfiguration
from hg_systematic.index.conversion import roll_schedule_to_tsd
from hg_systematic.index.pricing_service import price_index_op, IndexResult
from hg_systematic.index.units import IndexStructure, IndexPosition, NotionalUnitValues, NotionalUnits
from hg_systematic.operators import monthly_rolling_info, MonthlyRollingWeightRequest, monthly_rolling_weights, \
    rolling_contracts, price_in_dollars, MonthlyRollingInfo, calendar_for


@dataclass(frozen=True)
class MonthlySingleAssetIndexConfiguration(SingleAssetIndexConfiguration):
    """
    A single asset index that rolls monthly.

    roll_period: tuple[int, int]
        The first day of the roll and the last day of the roll.
        On the first day of the roll the index is re-balanced. The target position is deemed to be
        100% of the next contract. The first day can be specified as a negative offset, this will
        start n publishing days prior to the month rolling into. The second say is the last day of the
        roll and must be positive. On this day, the roll should be completed and the index will hold the
        contract specified for that month in the roll schedule.

        The days represent publishing days of the month, not the calendar day. So 1 (roll period day) may represent
        the 3 day of the calendar month if 1 and 2 were weekends.

        NOTE: A roll period cannot overlap with a prior roll period, so [-10,20] is not allowed as it would
              result in an overlap.

    roll_schedule: tuple[str, ...]
        The roll schedule for this index. This consists of 12 string entries (one for each month), each entry consists
        of a month (letter) and a single digit number representing the year offset for the roll. This will
        be either 0 or 1. For example: ["H0", ..., "X0", "F1"]
        This is used to indicate what contract should be the target for the month the roll period ends in.
        It is possible to specify the same contract, this will effectively be a non-rolling month then.

    roll_rounding: int
        The precision to round the rolling weights to.
    """
    roll_period: tuple[int, int] = None
    roll_schedule: tuple[str, ...] = None
    roll_rounding: int = 8
    trading_halt_calendar: str = None
    contract_fn: Callable[[str, int, int], str] = None


@graph(overloads=price_index_op)
def price_monthly_single_asset_index(config: TS[MonthlySingleAssetIndexConfiguration]) -> TSB[IndexResult]:
    """
    Support for a monthly rolling single asset index pricing logic.
    For now use the price_in_dollars service to get prices, but there is no reason to use specifically dollars as
    the index just needs a price, it is independent of the currency or scale.
    """
    # Prepare inputs
    monthly_rolling_request = combine[TS[MonthlyRollingWeightRequest]](
        start=config.roll_period[0],
        end=config.roll_period[1],
        calendar_name=config.publish_holiday_calendar,
        round_to=config.roll_rounding
    )

    halt_calendar = calendar_for(config.trading_halt_calendar)

    roll_info = monthly_rolling_info(monthly_rolling_request)
    rolling_weights = monthly_rolling_weights(monthly_rolling_request)
    roll_schedule = roll_schedule_to_tsd(config.roll_schedule)
    asset = config.asset
    contracts = rolling_contracts(
        roll_info,
        roll_schedule,
        asset,
        config.contract_fn
    )
    dt = roll_info.dt
    halt_trading = contains_(halt_calendar, dt)

    required_prices_fb = feedback(TSS[str], const({}, TSS[str]))
    # Join current positions + roll_in / roll_out contract, perhaps this could be reduced to just roll_in?
    all_contracts = union(convert[TSS](contracts), required_prices_fb())

    prices = map_(price_in_dollars, __keys__=all_contracts, __key_arg__="symbol")

    initial_structure_default = ...

    index_structure_fb = feedback(TSB[IndexStructure])
    index_structure = default(index_structure_fb(), initial_structure_default)

    out = monthly_single_asset_index_component(
        index_structure,
        rolling_weights,
        roll_info,
        contracts,
        prices,
        halt_trading
    )

    # We require prices for the items in the current position at least
    required_prices_fb(out.index_structure.current_position.units.key_set)
    index_structure_fb(out.index_structure)

    return out.level


@graph
def compute_level(
        current_position: TSB[IndexPosition],
        current_price: NotionalUnitValues
) -> TS[float]:
    """
    Compute the level from the current positions and the last re-balance level
    """
    return current_position.level + reduce(
        lambda x, y: x + y,
        map_(
            lambda pos_curr, prc_prev, prc_now: (prc_prev - prc_now) * pos_curr,
            current_position.units,
            current_position.value,
            no_key(current_price),
        ),
        0.0
    )


@graph
def target_units_from_current(
        current_contract: TS[str],
        current_units: TS[float],
        target_contract: TS[str],
        prices: NotionalUnitValues,
) -> TS[float]:
    """
    Compute the target units from the current contract unit using price weighting.
    """
    current_value = current_units * passive(prices[current_contract])
    return current_value / passive(prices[target_contract])


@graph
def roll_contracts(
        current_units: NotionalUnits,
        previous_units: NotionalUnits,
        previous_contract: TS[str],
        target_units: NotionalUnits,
        target_contract: TS[str],
        roll_weight: TS[float],
        roll_halted: TS[bool],
) -> NotionalUnits:
    """
    Converts the units from one contract to another.
    The ration of conversion is managed by the roll_weight.
    If we are in roll halt mode then we do not convert, but instead return the
    current units value.
    This produce a new set of current units from the combination of the previous and
    the target contracts. Roll is completed when the result matches the target units.
    """
    return switch_(
        roll_halted,
        {
            True: lambda c, p, p_c, t, t_c, w: c,
            False: lambda c, p, p_c, t, t_c, w: combine[NotionalUnits](
                keys=TSL[TS[str], Size[2]](p_c, t_c),
                values=TSL[TS[float], Size[2]](
                    p[p_c] * w,  # The remaining previous units
                    t[t_c] * (1.0 - w)  # The target units to move into
                )
            )
        },
        current_units,
        previous_units,
        previous_contract,
        target_units,
        target_contract,
        roll_weight
    )


@graph
def roll_completed(current_units: NotionalUnits, target_units: NotionalUnits) -> TS[bool]:
    return current_units == target_units


@graph
def monthly_single_asset_index_component(
        index_structure: TSB[IndexStructure],
        rolling_weights: TS[float],
        rolling_info: TSB[MonthlyRollingInfo],
        contracts: TSL[TS[str], Size[2]],
        prices: NotionalUnitValues,
        halt_trading: TS[bool]
) -> TSB[IndexResult]:
    """

    :param index_structure: The current index structure.
    :param rolling_weights: The weight to transition from previous to current position.
    :param rolling_info: The rolling information for this index.
    :param contracts: The contracts to roll from and to
    :param prices: The current price of the contracts of interest
    :param halt_trading: A signal to indicate that trading should be halted.
    :return: The level and other interim information.
    """

    needs_re_balance = or_(
        contracts[0] != contracts[1],
        not_(roll_completed(index_structure.current_position.units, index_structure.target_units))
    )
    new_index_structure = switch_(
        needs_re_balance,
        {
            True: lambda i_s, r_i, c, p, r_w, h_t: re_balance_contracts(i_s, r_i, c, p, r_w, h_t),
            False: lambda i_s, r_i, c, p, r_w, h_t: i_s
        },
        index_structure,
        rolling_info,
        contracts,
        prices,
        rolling_weights,
        halt_trading,
    )
    # If we have already traded this produces an unnecessary computation, but check if we traded again
    # may be just as expensive and there is less switching involved then.
    level = compute_level(new_index_structure.current_position, prices)
    return combine[TSB[IndexResult]](
        level=level,
        index_structure=new_index_structure
    )


def re_balance_contracts(
        index_structure: TSB[IndexStructure],
        rolling_info: TSB[MonthlyRollingInfo],
        contracts: TSL[TS[str], Size[2]],
        prices: NotionalUnitValues,
        rolling_weights: TS[float],
        halt_trading: TS[bool]
) -> TSB[IndexStructure]:
    # Compute the portfolio change
    re_balance_signal = rolling_info.begin_roll
    previous_units = sample(re_balance_signal, index_structure.current_position.units)
    target_units = sample(re_balance_signal, target_units_from_current(
        contracts[0],
        previous_units[contracts[0]],
        contracts[1],
        prices
    ))
    # Then we need to compute the time-related weighting when we are rolling
    current_units = switch_(
        rolling_info.roll_state,
        {
            CmpResult.LT: lambda c, p, p_c, t, t_c, w, h: c,
            CmpResult.EQ: lambda c, p, p_c, t, t_c, w, h: roll_contracts(c, p, p_c, t, t_c, w, h),
            CmpResult.GT: lambda c, p, p_c, t, t_c, w, h: if_then_else(h, c, t)
        },
        index_structure.current_position.units,
        previous_units,
        contracts[0],
        target_units,
        contracts[1],
        rolling_weights,
        halt_trading
    )
    # This will roll under normal circumstances, but it is possible that we remain un-transitioned
    # due to trading halts, so we put in protection for this case
    current_units = switch_(
        all_(
            CmpResult.LT == rolling_info.roll_state,
            len_(current_units) > 1,
            not halt_trading
        ),
        {
            True: lambda c, t, t_c: convert[NotionalUnits](t_c, t),
            False: lambda c, t, t_c: c
        },
        current_units,
        target_units,
        contracts[1]
    )

    # Detect "trade" and update the current positions to reflect said trade
    traded = current_units != index_structure.current_position.units
    current_position = switch_(
        traded,
        {
            True: lambda c, c_u, p: combine[IndexPosition](
                units=c_u,
                level=reduce(lambda x, y: x + y, map_(lambda a, b: a * b, c_u, no_key(p)), 0.0),
                unit_values=map_(lambda u, p: p, c_u, no_key(p))
            ),
            False: lambda c, c_u, p: c
        },
        index_structure.current_position,
        current_units,
        prices
    )

    # Detect the end-roll and adjust as appropriate
    end_roll = roll_completed(current_units, target_units)
    empty_units = const(frozendict(), NotionalUnits)
    previous_units = if_then_else(end_roll, empty_units, previous_units)
    target_units = if_then_else(end_roll, empty_units, target_units)

    return combine[TSB[IndexStructure]](
        current_position=current_position,
        previous_units=previous_units,
        target_units=target_units,
    )
