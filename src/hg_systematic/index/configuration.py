from dataclasses import dataclass
from datetime import date
from typing import Mapping

from frozendict import frozendict as fd
from hgraph import CompoundScalar


__all__ = ["IndexConfiguration", "SingleAssetIndexConfiguration", "MultiIndexConfiguration",]


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
    current_position: float = 0.0
    current_position_value: float = 0.0
    target_position: float = 0.0
    target_position_value: float = 0.0
    previous_position: float = 0.0
    previous_position_value: float = 0.0


@dataclass(frozen=True)
class MultiIndexConfiguration(IndexConfiguration):
    indices: tuple[str, ...] = None
    current_positions: Mapping[str, float] = fd()
    current_position_values: Mapping[str, float] = fd()
    target_positions: Mapping[str, float] = fd()
    target_position_values: Mapping[str, float] = fd()
    previous_positions: Mapping[str, float] = fd()
    previous_position_values: Mapping[str, float] = fd()
