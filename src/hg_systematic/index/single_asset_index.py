from dataclasses import dataclass
from typing import Callable

from frozendict import frozendict
from hgraph import graph, TS, combine, map_, TSB, Size, TSL, TSS, feedback, \
    const, union, no_key, reduce, if_then_else, switch_, CmpResult, len_, contains_, \
    default, debug_print, TSD, collect, not_, dedup, lag, or_, and_, gate

from hg_systematic.index.configuration import SingleAssetIndexConfiguration, initial_structure_from_config
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
    debug_print("monthly_rolling_request", monthly_rolling_request)

    halt_calendar = calendar_for(config.trading_halt_calendar)

    roll_info = monthly_rolling_info(monthly_rolling_request)
    debug_print("roll_info", roll_info)
    rolling_weights = monthly_rolling_weights(monthly_rolling_request)
    debug_print("rolling_weights", rolling_weights)

    roll_schedule = roll_schedule_to_tsd(config.roll_schedule)
    asset = config.asset
    contracts = rolling_contracts(
        roll_info,
        roll_schedule,
        asset,
        config.contract_fn
    )
    debug_print("contracts", contracts)

    dt = roll_info.dt
    halt_trading = dedup(contains_(halt_calendar, dt))
    debug_print("halt_trading", halt_trading)

    required_prices_fb = feedback(TSS[str], frozenset())
    # Join current positions + roll_in / roll_out contract, perhaps this could be reduced to just roll_in?
    all_contracts = union(combine[TSS[str]](*contracts), required_prices_fb())
    debug_print("all_contracts", all_contracts)

    prices = map_(lambda key: price_in_dollars(key), __keys__=all_contracts)
    debug_print("prices", prices)

    initial_structure_default = initial_structure_from_config(config)

    index_structure_fb = feedback(TSB[IndexStructure])
    debug_print("index_structure_fb", index_structure_fb())
    index_structure = dedup(default(lag(index_structure_fb(), 1, dt), initial_structure_default))
    debug_print("index_structure", index_structure)

    out = monthly_single_asset_index_component(
        index_structure,
        rolling_weights,
        roll_info,
        contracts,
        prices,
        halt_trading
    )
    debug_print("out", out)
    # We require prices for the items in the current position at least
    required_prices_fb(out.index_structure.current_position.units.key_set)
    index_structure_fb(dedup(out.index_structure))

    debug_print("level", out.level)
    return out


@graph
def compute_level(
        current_position: TSB[IndexPosition],
        current_price: NotionalUnitValues
) -> TS[float]:
    """
    Compute the level from the current positions and the last re-balance level
    """
    debug_print("current_positions.level", current_position.level)
    debug_print("compute_level:prices", current_price)
    debug_print("compute_level:units", current_position.units)
    debug_print("compute_level:unit_values", current_position.unit_values)
    returns = map_(
        lambda pos_curr, prc_prev, prc_now: (prc_now - prc_prev) * pos_curr,
        current_position.units,
        current_position.unit_values,
        current_price,
        __keys__=current_position.units.key_set,
    )
    debug_print("compute_level:returns", returns)
    new_level = current_position.level + reduce(
        lambda x, y: x + y,
        returns,
        0.0
    )
    debug_print("compute_level:new_level", new_level)
    return new_level


@graph
def target_units_from_current(
        level: TS[float],
        target_contract: TS[str],
        prices: NotionalUnitValues,
) -> TSD[str, TS[float]]:
    """
    Compute the target units from the current contract unit using price weighting.
    """
    target_units = level / prices[target_contract]
    return collect[TSD](target_contract, target_units)


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
            False: lambda c, p, p_c, t, t_c, w: _roll_contracts(p, t, w)
        },
        current_units,
        previous_units,
        previous_contract,
        target_units,
        target_contract,
        roll_weight
    )


@graph
def _roll_contracts(prev_units: NotionalUnits, target_units: NotionalUnits, weights: TS[float]) -> NotionalUnits:
    prev = map_(lambda u, w: u * w, prev_units, weights)
    target = map_(lambda u, w: u * (1.0 - w), target_units, weights)
    return map_(lambda p, t: default(p, 0.0) + default(t, 0.0), prev, target)


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

    needs_re_balance = dedup(or_(
        # This will initiate a roll, so will set the target units
        rolling_info.as_schema.begin_roll,
        # Once the roll is complete, the target units are set to an empty dict.
        len_(index_structure.target_units) > 0,
    ))

    # If we have already traded this produces an unnecessary computation, but check if we traded again
    # may be just as expensive and there is less switching involved then.
    level = compute_level(index_structure.current_position, prices)

    debug_print("needs_re_balance", needs_re_balance)
    debug_print("index_structure: pre", index_structure)
    new_index_structure = switch_(
        needs_re_balance,
        {
            True: lambda i_s, r_i, c, p, r_w, h_t, l: re_balance_contracts(i_s, r_i, c, p, r_w, h_t, l),
            False: lambda i_s, r_i, c, p, r_w, h_t, l: i_s  # Force a copy of the value when switching
        },
        index_structure,
        rolling_info,
        contracts,
        prices,
        rolling_weights,
        halt_trading,
        level
    )
    debug_print("new_index_structure", new_index_structure)

    out = combine[TSB[IndexResult]](
        level=level,
        index_structure=new_index_structure
    )
    debug_print("out:result", out)
    return out


@graph
def re_balance_contracts(
        index_structure: TSB[IndexStructure],
        rolling_info: TSB[MonthlyRollingInfo],
        contracts: TSL[TS[str], Size[2]],
        prices: NotionalUnitValues,
        rolling_weights: TS[float],
        halt_trading: TS[bool],
        level: TS[float],
) -> TSB[IndexStructure]:
    # Compute the portfolio change
    re_balance_signal = rolling_info.begin_roll
    debug_print("re_balance_signal", re_balance_signal)
    previous_units = if_then_else(re_balance_signal, index_structure.current_position.units,
                                  index_structure.previous_units)
    debug_print("previous_units", previous_units)
    target_units = if_then_else(re_balance_signal, target_units_from_current(
        level,
        contracts[1],
        prices
    ), index_structure.target_units)
    debug_print("target_units", target_units)
    # Then we need to compute the time-related weighting when we are rolling
    current_units = switch_(
        rolling_info.roll_state,
        {
            CmpResult.LT: lambda c, p, p_c, t, t_c, w, h: if_then_else(and_(len_(c) > 1, not_(halt_trading)), t, c),
            CmpResult.EQ: lambda c, p, p_c, t, t_c, w, h: roll_contracts(c, p, p_c, t, t_c, w, h),
            CmpResult.GT: lambda c, p, p_c, t, t_c, w, h: if_then_else(h, c, t)
        },
        index_structure.current_position.units,
        previous_units,
        contracts[0],
        target_units,
        contracts[1],
        rolling_weights,
        halt_trading,
    )
    debug_print("current_units:1", current_units)
    # This will roll under normal circumstances, but it is possible that we remain un-transitioned
    # due to trading halts, so we put in protection for this case

    # Detect "trade" and update the current positions to reflect said trade
    traded = not_(current_units == index_structure.current_position.units)
    debug_print("traded", traded)
    current_position = switch_(
        traded,
        {
            True: lambda c_p, c_u, p, l: combine[TSB[IndexPosition]](
                units=c_u,
                level=l,
                unit_values=map_(lambda u, p: p, c_u, no_key(p))
            ),
            False: lambda c_p, c_u, p, l: c_p
        },
        index_structure.current_position,
        current_units,
        prices,
        level
    )
    debug_print("current_position:3", current_position)

    # Detect the end-roll and adjust as appropriate

    end_roll = gate(not_(halt_trading), rolling_info.as_schema.end_roll)
    debug_print("end_roll", end_roll)
    empty_units = const(frozendict(), NotionalUnits)
    # When the current_units match the target units, we are done, reset the target and previous states.
    previous_units = if_then_else(end_roll, empty_units, previous_units)
    target_units = if_then_else(end_roll, empty_units, target_units)
    debug_print("previous_units:final", previous_units)
    debug_print("target_units:final", target_units)
    debug_print("current_position:final", current_position)
    return combine[TSB[IndexStructure]](
        current_position=current_position,
        previous_units=previous_units,
        target_units=target_units,
    )
