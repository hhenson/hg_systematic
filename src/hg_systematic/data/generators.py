import math
from datetime import timedelta, datetime
from zoneinfo import ZoneInfo

import numpy as np
from hg_oap.dates.dt_utils import date_tz_to_utc
from hgraph import generator, TS, EvaluationEngineApi


@generator
def white_noise_generator(
        loc: float = 0.0,
        scale: float = 1.0,
        frequency: timedelta = timedelta(days=1),
        offset: timedelta = timedelta(),
        timezone: str = None,
        _api: EvaluationEngineApi = None
) -> TS[float]:
    """Generates a stream of white noise with a regular frequency"""
    size = math.ceil(((end := _api.end_time) - (start := _api.start_time)) / frequency) + 1
    samples = iter(np.random.normal(loc, scale, size=size))
    current = date_tz_to_utc(start.date(), ZoneInfo(timezone)) if timezone else datetime(start.year, start.month,
                                                                                         start.day)
    current += offset
    while current < start:
        current += frequency
    while current <= end:
        yield current, float(next(samples))
        current += frequency
