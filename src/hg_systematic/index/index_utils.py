from typing import TypeVar, Callable

from frozendict import frozendict
from hgraph import graph, TSB, TS, map_, reduce, dedup, or_, and_, len_, DebugContext, combine, switch_, TS_SCHEMA, \
    sample, default, gate, not_, if_then_else, CmpResult, no_key, const

from hg_systematic.index.configuration import IndexConfiguration
from hg_systematic.index.pricing_service import IndexResult
from hg_systematic.index.units import IndexPosition, NotionalUnitValues, IndexStructure, NotionalUnits
from hg_systematic.operators import MonthlyRollingInfo, monthly_rolling_info, monthly_rolling_weights, \
    MonthlyRollingWeightRequest


ROLLING_CONFIG = TypeVar("ROLLING_CONFIG", bound=IndexConfiguration)

@graph
def monthly_rolling_index_component(
        config: TS[ROLLING_CONFIG],
        index_structure: TSB[IndexStructure],
        rolling_weights: TS[float],
        rolling_info: TSB[MonthlyRollingInfo],
        prices: NotionalUnitValues,
        halt_trading: TS[bool],
        re_balance_signal_fn: Callable[[TSB[TS_SCHEMA]], TS[bool]],
        compute_target_units_fn: Callable[[TSB[TS_SCHEMA]], NotionalUnitValues],
        **kwargs: TSB[TS_SCHEMA]
) -> TSB[IndexResult]:
    """
    :param config: The configuration for this index.
    :param index_structure: The current index structure.
    :param rolling_weights: The weight to transition from previous to current position.
    :param rolling_info: The rolling information for this index.
    :param prices: The current price of the contracts of interest
    :param halt_trading: A signal to indicate that trading should be halted.
    :return: The level and other interim information.
    """

    # If we have already traded this produces an unnecessary computation, but check if we traded again
    # may be just as expensive and there is less switching involved then.
    level = compute_level(index_structure.current_position, prices)

    new_index_structure = re_balance_index(
        config=config,
        index_structure = index_structure,
        roll_info = rolling_info,
        roll_weight = rolling_weights,
        prices=prices,
        level=level,
        trade_halt=halt_trading,
        # We re-balance every time the begin_roll is triggered irrespective of trade-halt conditions.
        re_balance_signal_fn=re_balance_signal_fn,
        compute_target_units_fn=compute_target_units_fn,
        **kwargs
    )

    return combine[TSB[IndexResult]](
        level=level,
        index_structure=new_index_structure
    )


@graph
def compute_level(
        current_position: TSB[IndexPosition],
        current_value: NotionalUnitValues
) -> TS[float]:
    """
    Compute the level from the current positions and the last re-balance level
    """
    DebugContext.print("[compute_level] current_positions", current_position)
    DebugContext.print("[compute_level] compute_value", current_value)
    returns = map_(
        lambda pos_curr, prc_prev, prc_now: (prc_now - prc_prev) * pos_curr,
        current_position.units,
        current_position.unit_values,
        current_value,
        __keys__=current_position.units.key_set,
    )
    DebugContext.print("[compute_level] returns", returns)
    new_level = current_position.level + reduce(
        lambda x, y: x + y,
        returns,
        0.0
    )
    DebugContext.print("[compute_level] level", new_level)
    return new_level


@graph
def needs_re_balance(
        index_structure: TSB[IndexStructure],
        rolling_info: TSB[MonthlyRollingInfo],
        re_balance_signal: TS[bool] = True
) -> TS[bool]:
    """
    Determines if the index needs to be re-balanced. A filter condition can be supplied to restrict
    when the re-balance is actually triggered, by default the value is set to True.
    This will turn True when the rolling info begin_roll is set to True, and will remain true until
    the target_units property of the index structure is set back to an empty set.

    The re_balance_signal is defaulted to True. If set, this must be True when the begin_roll is True in
    order to trigger the re-balance.
    """
    re_balance = dedup(or_(
        # This will initiate a roll, so will set the target units
        and_(rolling_info.as_schema.begin_roll, re_balance_signal),
        # Once the roll is complete, the target units are set to an empty dict.
        len_(index_structure.target_units) > 0,
    ))
    DebugContext.print("[needs_re_balance]", re_balance)
    return re_balance


# Check that the type includes the roll_period and roll_rounding values to be safe
@graph(requires=lambda m, s: {"roll_period", "roll_rounding"}.issubset(m[ROLLING_CONFIG].meta_data_schema))
def get_monthly_rolling_values(config: TS[ROLLING_CONFIG]) -> TSB["roll_info": TSB[MonthlyRollingInfo],
                                                              "weights": TS[float]]:
    monthly_rolling_request = combine[TS[MonthlyRollingWeightRequest]](
        start=config.roll_period[0],
        end=config.roll_period[1],
        calendar_name=config.publish_holiday_calendar,
        round_to=config.roll_rounding
    )
    DebugContext.print("[monthly_rolling] request", monthly_rolling_request)
    roll_info = monthly_rolling_info(monthly_rolling_request)
    DebugContext.print("[monthly_rolling] roll_info", roll_info)
    rolling_weights = monthly_rolling_weights(monthly_rolling_request)
    DebugContext.print("[monthly_rolling] rolling_weights", rolling_weights)
    return combine[TSB](roll_info=roll_info, weights=rolling_weights)


@graph
def re_balance_index(
        config: TS[ROLLING_CONFIG],
        index_structure: TSB[IndexStructure],
        roll_info: TSB[MonthlyRollingInfo],
        roll_weight: TS[float],
        prices: NotionalUnitValues,  # Uses prices instead of values to avoid conflict with values() method
        level: TS[float],
        trade_halt: TS[bool],
        re_balance_signal_fn: Callable[[TSB[TS_SCHEMA]], TS[bool]],
        compute_target_units_fn: Callable[[TSB[TS_SCHEMA]], NotionalUnitValues],
        **kwargs: TSB[TS_SCHEMA]
) -> TSB[IndexStructure]:
    """
    Re-balance the index when needs re-balance is True.
    Will call the re-balance function with the schema:
        * config
        * index_structure
        * roll_info
        * roll_weights
        * re_balance_signal
        * prices
        * level
        * trade_halt
        * **kwargs (expansion of any additional arguments provided)

    The function should be a graph or compute node that takes the form of:

    ::
        @graph
        def my_re_balance_fn(tsb: TSB[TS_SCHEMA]) -> TSB[IndexStructure]:
            ...


    """
    DebugContext.print("[re_balance_index] index_structure:pre", index_structure)

    re_balance_signal = re_balance_signal_fn(combine[TSB](
        config=config,
        index_structure=index_structure,
        roll_info=roll_info,
        roll_weight=roll_weight,
        prices=prices,
        level=level,
        trade_halt=trade_halt,
        **kwargs,
    ))

    new_index_structure = switch_(
        needs_re_balance(index_structure, roll_info, re_balance_signal),
        {
            True: lambda tsb: _re_balance(tsb, compute_target_units_fn),
            False: _pass_through,
        },
        combine[TSB](
            config=config,
            index_structure=index_structure,
            re_balance_signal=re_balance_signal,
            roll_info=roll_info,
            roll_weight=roll_weight,
            prices=prices,
            level=level,
            trade_halt=trade_halt,
            **kwargs,
        )
    )
    DebugContext.print("[re_balance_index] index_structure:post", new_index_structure)
    return new_index_structure


@graph
def _pass_through(tsb: TSB[TS_SCHEMA]) -> TSB[IndexStructure]:
    return tsb.index_structure


@graph
def _re_balance(
        tsb: TSB[TS_SCHEMA],
        extract_target_units_fn: Callable[[TSB[TS_SCHEMA]], NotionalUnitValues]
) -> TSB[IndexStructure]:
    # Ensure we have a valid value when we enter (This should only enter initially when we re-balance)
    end_roll = dedup(
        sample(
            (dt := (roll_info := tsb.roll_info).dt),
            default(
                gate(not_(trade_halt := tsb.trade_halt), roll_info.as_schema.end_roll),
                False
            )
        )
    )
    DebugContext.print("[re_balance_index] end_roll", end_roll)
    DebugContext.print("[re_balance_index] re_balance_signal", re_balance_signal := tsb.re_balance_signal)
    DebugContext.print("[re_balance_index] roll_weight", roll_weight := tsb.roll_weight)

    # Compute the portfolio change
    previous_units = if_then_else(
        re_balance_signal,
        (current_units := (current_position := (index_structure := tsb.index_structure).current_position).units),
        index_structure.previous_units
    )
    DebugContext.print("[re_balance_index] previous_units", previous_units)

    target_units = if_then_else(  # Try and replace with switch
        re_balance_signal,
        extract_target_units_fn(
            tsb
        ),
        index_structure.target_units
    )
    DebugContext.print("[re_balance_index] target_units", target_units)

    # Then we need to compute the time-related weighting when we are rolling
    rolled_units = switch_(
        roll_info.roll_state,
        {
            CmpResult.LT: lambda c, p, t, w, h, e_r: if_then_else(e_r, t, c),
            CmpResult.EQ: lambda c, p, t, w, h, e_r: roll_units(c, p, t, w, h),
            CmpResult.GT: lambda c, p, t, w, h, e_r: if_then_else(h, c, t)
        },
        current_units,
        previous_units,
        target_units,
        roll_weight,
        trade_halt,
        end_roll
    )
    DebugContext.print("[re_balance_index] rolled_units", rolled_units)

    # Detect "trade" and update the current positions to reflect said trade
    traded = not_(rolled_units == current_units)
    DebugContext.print("traded", traded)
    rolled_position = switch_(
        traded,
        {
            True: lambda c_p, c_u, p, l: combine[TSB[IndexPosition]](
                units=c_u,
                level=l,
                unit_values=map_(lambda u, p: p, c_u, no_key(p))
            ),
            False: lambda c_p, c_u, p, l: c_p
        },
        current_position,
        current_units,
        tsb.prices,
        tsb.level
    )
    DebugContext.print("[re_balance_index] rolled_position", rolled_position)

    # Detect the end-roll and adjust as appropriate
    empty_units = const(frozendict(), NotionalUnits)

    return combine[TSB[IndexStructure]](
        current_position=rolled_position,
        previous_units=if_then_else(end_roll, empty_units, previous_units),
        target_units=if_then_else(end_roll, empty_units, target_units),
    )


@graph
def roll_units(
        current_units: NotionalUnits,
        previous_units: NotionalUnits,
        target_units: NotionalUnits,
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
            True: lambda c, p, t, w: c,
            False: lambda c, p, t, w: _roll_units(p, t, w)
        },
        current_units,
        previous_units,
        target_units,
        roll_weight
    )


@graph
def _roll_units(prev_units: NotionalUnits, target_units: NotionalUnits, weights: TS[float]) -> NotionalUnits:
    prev = map_(lambda u, w: u * w, prev_units, weights)
    target = map_(lambda u, w: u * (1.0 - w), target_units, weights)
    return map_(lambda p, t: default(p, 0.0) + default(t, 0.0), prev, target)
