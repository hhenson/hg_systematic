"""
Representing the units held (or desired to be held) by an index.
"""
from dataclasses import dataclass

from hgraph import TSD, TS, TimeSeriesSchema, graph, subscription_service, TSS, service_impl, mesh_, TSB

__all__ = ["NotionalUnits", "NotionalUnitValues", "IndexPosition", "IndexStructure",]

# A dictionary of fractional units representing the current or desired holding of the unit
NotionalUnits = TSD[str, TS[float]]

# A dictionary of floating point numbers that represents the value of the units (this can be in notional in the case of
# a SingleAssetIndex or levels in the case of a MultiIndex).
NotionalUnitValues = TSD[str, TS[float]]
# NOTE: Each time an index is modified the value of the units needs to be set correctly.


@dataclass
class IndexPosition(TimeSeriesSchema):
    """
    The units and value of the units as well as the level of the index
    when the units were set.
    """
    units: NotionalUnits
    unit_values: NotionalUnitValues
    level: TS[float]


@dataclass
class IndexStructure(TimeSeriesSchema):
    """
    This three-phase structure allows us to represent the state of the index as well as track the roll period.
    We set the previous_position to the current_position values on the re-balance signal as well as setting the
    target_position to the desired position we wish the index to hold after the roll period.

    Once the roll is completed, these can be released (or left as is if desired).

    We price the index based on the current position and the difference between the current value of the underlyer
    and the current_position's value.

    The value does not change when no re-balancing is occurring.
    """
    current_position: TSB[IndexPosition]
    target_units: NotionalUnits
    previous_units: NotionalUnits


