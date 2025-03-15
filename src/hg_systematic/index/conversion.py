from typing import Mapping
from frozendict import frozendict as fd
from hg_oap.instruments.future import month_from_code
from hgraph import lift, TSD, TS

__all__ = ["roll_schedule_to_map", "roll_schedule_to_tsd"]

def roll_schedule_to_map(roll_schedule: tuple[str, ...]) -> Mapping[int, tuple[int, int]]:
    """convert a roll schedule tuple to a mapping"""
    assert all(item[1] in ('0', '1') for item in roll_schedule), "Roll year must be 0 or 1"
    return fd({ndx+1: (month_from_code(item[0]), int(item[1])) for ndx, item in enumerate(roll_schedule)})


roll_schedule_to_tsd = lift(roll_schedule_to_map, output=TSD[int, TS[tuple[int, int]]])
