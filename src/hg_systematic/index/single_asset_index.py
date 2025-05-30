from contextlib import nullcontext
from dataclasses import dataclass
from typing import Callable

from hgraph import graph, TS, combine, map_, TSB, TSS, feedback, \
    union, TSD, dedup, sample, last_modified_date, convert, dispatch, TSL, Size, nothing
from hgraph import if_true, DebugContext

from hg_systematic.index.configuration import SingleAssetIndexConfiguration
from hg_systematic.index.conversion import roll_schedule_to_tsd
from hg_systematic.index.index_utils import get_monthly_rolling_values, monthly_rolling_index
from hg_systematic.index.pricing_service import price_index_op, IndexResult
from hg_systematic.operators import futures_rolling_contracts, price_in_dollars, MonthlyRollingInfo
from hg_systematic.operators._rolling_rules import spread_rolling_contracts


__all__ = [
    "price_monthly_single_asset_index", "MonthlySingleAssetIndexConfiguration",
    "MonthlySpreadSingleAssetIndexConfiguration", "rolling_contract", "rolling_spread_contract" ,
    "set_single_index_debug_on",
]


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


@dataclass(frozen=True)
class MonthlySpreadSingleAssetIndexConfiguration(MonthlySingleAssetIndexConfiguration):
    """
    The spread uses the near leg to be the roll_schedule, the far leg is defined using the far leg roll schedule.
    """
    far_leg_roll_schedule: tuple[str, ...] = None
    contract_fn: Callable[[str, int, int, int, int], str] = None


@dispatch(on=("config",))
def rolling_contract(config: TS[MonthlySingleAssetIndexConfiguration], asset: TS[str],
                     roll_info: TSB[MonthlyRollingInfo]) -> TSL[TS[str], Size[2]]:
    roll_schedule = roll_schedule_to_tsd(config.roll_schedule)

    return futures_rolling_contracts(
        roll_info,
        roll_schedule,
        asset,
        config.contract_fn
    )


@graph(overloads=rolling_contract)
def rolling_spread_contract(config: TS[MonthlySingleAssetIndexConfiguration], asset: TS[str],
                     roll_info: TSB[MonthlyRollingInfo]) -> TSL[TS[str], Size[2]]:
    roll_schedule = roll_schedule_to_tsd(config.roll_schedule)
    far_roll_schedule = roll_schedule_to_tsd(config.far_leg_roll_schedule)

    return spread_rolling_contracts(
        roll_info,
        roll_schedule,
        far_roll_schedule,
        asset,
        config.contract_fn
    )


@graph(overloads=price_index_op)
def price_monthly_single_asset_index(config: TS[MonthlySingleAssetIndexConfiguration]) -> TSB[IndexResult]:
    """
    Support for a monthly rolling single asset index pricing logic.
    For now use the price_in_dollars service to get prices, but there is no reason to use specifically dollars as
    the index just needs a price, it is independent of the currency or scale.
    """
    with nullcontext() if DEBUG_ON is False else DebugContext("[SingleIndex]"):
        DebugContext.print("Starting", config.symbol)
        DebugContext.print("Asset", asset := config.asset)

        # We need the roll_info to compute the contracts, so we get it here
        roll_info, roll_weight = get_monthly_rolling_values(config)

        contracts = rolling_contract(config, asset, roll_info)
        DebugContext.print("contracts", contracts)

        dt = roll_info.dt

        required_prices_fb = feedback(TSS[str], frozenset())
        # Join current positions + roll_in / roll_out contract, perhaps this could be reduced to just roll_in?
        all_contracts = union(combine[TSS[str]](*contracts), required_prices_fb())
        DebugContext.print("all_contracts", all_contracts)

        prices = map_(lambda key, dt_: sample(if_true(dt_ >= last_modified_date(p := price_in_dollars(key))), p),
                      __keys__=all_contracts, dt_=dt)
        DebugContext.print("prices", prices)

        out = monthly_rolling_index(
            config,
            prices,
            compute_target_units_fn=lambda tsb: convert[TSD](target_contract := tsb.contracts[1],
                                                             tsb.level / tsb.prices[target_contract]),
            roll_weight=roll_weight,  # Supplied to reduce unnecessary computation.
            roll_info=roll_info,  # Supplied to reduce unnecessary computation.
            contracts=contracts,
        )

        # We require prices for the items in the current position at least
        required_prices_fb(out.index_structure.current_position.units.key_set)

        return out
