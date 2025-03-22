from contextlib import nullcontext
from dataclasses import dataclass
from typing import Callable

from frozendict import frozendict
from hgraph import debug_print as _debug_print, if_true, DebugContext, TS_SCHEMA
from hgraph import graph, TS, combine, map_, TSB, Size, TSL, TSS, feedback, \
    const, union, no_key, if_then_else, switch_, CmpResult, len_, contains_, \
    default, TSD, not_, dedup, lag, or_, gate, sample, last_modified_date, convert

from hg_systematic.index.configuration import SingleAssetIndexConfiguration, initial_structure_from_config
from hg_systematic.index.conversion import roll_schedule_to_tsd
from hg_systematic.index.index_utils import compute_level, get_monthly_rolling_values, needs_re_balance, \
    re_balance_index, monthly_rolling_index_component
from hg_systematic.index.pricing_service import price_index_op, IndexResult
from hg_systematic.index.units import IndexStructure, IndexPosition, NotionalUnitValues, NotionalUnits
from hg_systematic.operators import monthly_rolling_info, MonthlyRollingWeightRequest, monthly_rolling_weights, \
    rolling_contracts, price_in_dollars, MonthlyRollingInfo, calendar_for

DEBUG_ON = False


def set_single_index_debug_on():
    global DEBUG_ON
    DEBUG_ON = True


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
    with nullcontext() if DebugContext.instance() is not None or DEBUG_ON is False else DebugContext("[SingleIndex]"):
        halt_calendar = calendar_for(config.trading_halt_calendar)
        roll_info, rolling_weights = get_monthly_rolling_values(config)
        roll_schedule = roll_schedule_to_tsd(config.roll_schedule)
        asset = config.asset

        contracts = rolling_contracts(
            roll_info,
            roll_schedule,
            asset,
            config.contract_fn
        )
        DebugContext.print("contracts", contracts)

        dt = roll_info.dt
        halt_trading = dedup(contains_(halt_calendar, dt))
        DebugContext.print("halt_trading", halt_trading)

        required_prices_fb = feedback(TSS[str], frozenset())
        # Join current positions + roll_in / roll_out contract, perhaps this could be reduced to just roll_in?
        all_contracts = union(combine[TSS[str]](*contracts), required_prices_fb())
        DebugContext.print("all_contracts", all_contracts)

        prices = map_(lambda key, dt_: sample(if_true(dt_ >= last_modified_date(p := price_in_dollars(key))), p), __keys__=all_contracts, dt_=dt)
        DebugContext.print("prices", prices)

        initial_structure_default = initial_structure_from_config(config)

        index_structure_fb = feedback(TSB[IndexStructure])
        DebugContext.print("index_structure_fb", index_structure_fb())
        index_structure = dedup(default(lag(index_structure_fb(), 1, dt), initial_structure_default))
        DebugContext.print("index_structure", index_structure)

        out = monthly_rolling_index_component(
            config,
            index_structure,
            rolling_weights,
            roll_info,
            prices,
            halt_trading,
            re_balance_signal_fn=lambda tsb: tsb.roll_info.begin_roll,
            compute_target_units_fn=lambda tsb: convert[TSD](target_contract:=tsb.contracts[1], tsb.level / tsb.prices[target_contract]),
            contracts=contracts,
        )
        # This could be triggered due to prices ticking on non-publishing days, we only want results that are for the
        # publishing dates.
        DebugContext.print("computed_result", out)
        # We require prices for the items in the current position at least
        required_prices_fb(out.index_structure.current_position.units.key_set)
        # There is a dedup here as there seems to be a bug somewhere when dealing with REFs and TSD, will trace down later.
        index_structure_fb(dedup(out.index_structure))

        DebugContext.print("level", out.level)
        return out


