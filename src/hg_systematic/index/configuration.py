from dataclasses import dataclass
from datetime import date
from typing import Mapping

from frozendict import frozendict as fd
from hgraph import CompoundScalar, compute_node, TS, TSB

__all__ = ["IndexConfiguration", "SingleAssetIndexConfiguration", "MultiIndexConfiguration", "initial_structure_from_config"]

from hg_systematic.index.units import IndexStructure, NotionalUnits


@dataclass(frozen=True)
class IndexConfiguration(CompoundScalar):
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
    publish_holiday_calendar: str = None
    rounding: int = 8
    initial_level: float = 100.0
    start_date: date = None
    current_position: Mapping[str, float] = None
    current_position_value: Mapping[str, float] = None
    current_level: float = 100.0
    target_position: Mapping[str, float] = None
    previous_position: Mapping[str, float] = None


@dataclass(frozen=True)
class SingleAssetIndexConfiguration(IndexConfiguration):
    """
    In order to set appropriate initial conditions, the position data is available to be set.

    asset: str
        The asset symbol. Used to construct the contract name.

    initial_level: float
        Defaulted to 100.0
        If this is expected to start from a positions within the stream of index values, then the initial
        conditions for the positions tracking is also required.
    """
    asset: str = None


@dataclass(frozen=True)
class MultiIndexConfiguration(IndexConfiguration):
    indices: tuple[str, ...] = None


@compute_node
def initial_structure_from_config(config: TS[IndexConfiguration]) -> TSB[IndexStructure]:
    """
    Prepare the initial structure from the index configuration.
    This will tick once only with the values extracted from the index configuration.
    """
    config.make_passive()
    config: IndexConfiguration = config.value
    return {
        "current_position": {
            "units": {} if config.current_position is None else config.current_position,
            "unit_values": {} if config.current_position is None else config.current_position_value,
            "level": config.current_level
        },
        "previous_units": {} if config.previous_position is None else config.previous_position,
        "target_units": {} if config.target_position is None else config.target_position,
    }
