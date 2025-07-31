from dataclasses import dataclass
from datetime import date
from typing import Mapping

from frozendict import frozendict as fd
from hgraph import CompoundScalar, compute_node, TS, TSB, graph, switch_, dispatch, const, convert, TSD, \
    map_, combine, TSS, reduce, add_, take, nothing, dedup

from hg_systematic.index.units import IndexStructure

__all__ = ["IndexConfiguration", "StubIndexConfiguration", "BaseIndexConfiguration", "SingleAssetIndexConfiguration",
           "MultiIndexConfiguration", "initial_structure_from_config"]

from hg_systematic.operators import price_in_dollars, trade_date


@dataclass(frozen=True)
class IndexConfiguration(CompoundScalar):
    symbol: str
    rounding: int = 8
    start_date: date = None  # Required to ensure we don't try and compute an index before it has started.
    publish_holiday_calendar: str = None  # Required to know when we can publish a value for an index.


@dataclass(frozen=True)
class BaseIndexConfiguration(IndexConfiguration):
    """

    publish_holiday_calendar: str
        The calendar to use for publishing the index.

    rounding: int
        The number of decimal places to round the published result to

    initial_level: float
        The level to start the index at.

    start_date: date
        The first date of the index. Since the level is path dependent, the start date is required.
    """
    symbol: str
    initial_level: float = 100.0
    current_position: Mapping[str, float] = None
    current_position_value: Mapping[str, float] = None
    current_level: float = 100.0
    target_position: Mapping[str, float] = None
    previous_position: Mapping[str, float] = None


@dataclass(frozen=True)
class StubIndexConfiguration(IndexConfiguration):
    """
    A stub index is one that has a price that can be retrieved using a generic price service.
    This does not provide any initial conditions.
    """


@dataclass(frozen=True)
class SingleAssetIndexConfiguration(BaseIndexConfiguration):
    """
    In order to set appropriate initial conditions, the position data is available to be set.

    asset: str
        The asset symbol. Used to construct the contract name.

    initial_contract: str
        The initial contract to consider as holding on day one. This is used to key the current position and
        the price to construct the percentage holding to compute the initial level.
    """
    asset: str = None
    initial_contract: str = None


@dataclass(frozen=True)
class MultiIndexConfiguration(BaseIndexConfiguration):
    indices: tuple[str, ...] = None


@graph
def initial_structure_from_config(config: TS[IndexConfiguration]) -> TSB[IndexStructure]:
    td = trade_date()
    dt = take(td, 1)
    # This should only compute something on day one. After that it should no longer do anything.
    out = switch_(
        dt == td,
        {
            True: lambda dt_, config_: switch_(
                dt_ == config_.start_date,
                {
                    False: recover_initial_structure_from_config,
                    True: compute_initial_structure,
                },
                config_,
            ),
            False: lambda dt_, config_: nothing(TSB[IndexStructure]),
        },
        dt,
        config,
    )
    # Ensure we have a stable result when we switch to the nothing branch for consumers of the data.
    return dedup(out)


_DEFAULT_VALUE = fd({
    "current_position": {
        "units": {},
        "unit_values": {},
        "level": 100.0
    },
    "previous_units": {},
    "target_units": {},
})


@dispatch(on=("config",))
def recover_initial_structure_from_config(config: TS[IndexConfiguration]) -> TSB[IndexStructure]:
    """
    Recover the index structure from the current state.
    The config may contain the necessary information.
    By default, the empty structure is returned. This will not make the code happy!
    """
    return const(_DEFAULT_VALUE, TSB[IndexStructure])


@dispatch(on=("config",))
def compute_initial_structure(config: TS[IndexConfiguration]) -> TSB[IndexStructure]:
    """
    Create the initial index structure from the current state.
    The config may contain the necessary information.
    By default, the empty structure is returned. This will not make the code happy!
    """
    return const(_DEFAULT_VALUE, TSB[IndexStructure])


@graph(overloads=compute_initial_structure)
def compute_initial_structure_from_single_asset(config: TS[SingleAssetIndexConfiguration]) -> TSB[IndexStructure]:
    """Computes a structure for a single asset index."""
    asset = convert[TSS](config.initial_contract)
    initial_level = config.initial_level
    # TODO: Should provide a version which is just price or price in native currency, ...
    unit_values = map_(lambda key: price_in_dollars(key), __keys__=asset)
    total_value = reduce(add_, unit_values, 0.0)
    units = map_(lambda r: r, initial_level / total_value, __keys__=asset)
    # Wait until we have all prices before ticking out the data as a single tick.
    return combine[TSB[IndexStructure]](
        current_position=combine(
            units=units,
            unit_values=unit_values,
            level=config.initial_level
        ),
        previous_units=(c := const(fd(), TSD[str, TS[float]])),
        target_units=c,
    )


@graph(overloads=compute_initial_structure)
def compute_initial_structure_from_multi_asset(config: TS[MultiIndexConfiguration]) -> TSB[IndexStructure]:
    """Computes the initial index structure from the indices"""
    from hg_systematic.index.pricing_service import price_index
    indices = convert[TSS](config.indices)
    initial_level = config.initial_level
    # TODO: Should provide a version which is just price or price in native currency, ...
    unit_values = map_(lambda key: price_index(key).level, __keys__=indices)
    total_value = reduce(add_, unit_values, 0.0)
    # Compute a simple price-weighted contribution to initialise with.
    units = map_(lambda r: r, initial_level / total_value, __keys__ = indices)
    # Wait until we have all prices before ticking out the data as a single tick.
    return combine[TSB[IndexStructure]](
        current_position=combine(
            units=units,
            unit_values=unit_values,
            level=config.initial_level
        ),
        previous_units=(c := const(fd(), TSD[str, TS[float]])),
        target_units=c,
    )


@compute_node(overloads=recover_initial_structure_from_config)
def recover_initial_structure_from_config_for_base_index(config: TS[BaseIndexConfiguration]) -> TSB[IndexStructure]:
    """
    Prepare the initial structure from the index configuration.
    This will tick once only with the values extracted from the index configuration.
    """
    config.make_passive()
    config: BaseIndexConfiguration = config.value
    if config.current_position is None or config.current_position_value is None:
        raise ValueError(
            "When using the BaseIndexConfiguration default for recovering, the current_position and current_position_value may not be None.")
    return {
        "current_position": {
            "units": config.current_position,
            "unit_values": config.current_position_value,
            "level": config.current_level
        },
        "previous_units": {} if config.previous_position is None else config.previous_position,
        "target_units": {} if config.target_position is None else config.target_position,
    }
