import math
from datetime import timedelta, datetime
from zoneinfo import ZoneInfo

import numpy as np
from hg_oap.dates.dt_utils import date_tz_to_utc
from hgraph import generator, TS, EvaluationEngineApi, graph, lag, delayed_binding, feedback, compute_node, \
    RECORDABLE_STATE, TimeSeriesSchema


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


class ARState(TimeSeriesSchema):
    previous_terms: TS[tuple[float,...]]

@compute_node
def auto_regressive_generator(
        white_noise: TS[float],
        order: int = 1,
        initial_values: tuple[float, ...] = (1.0,),
        coefficients: tuple[float, ...] = (1.0, 0.5),
        _state: RECORDABLE_STATE[ARState] = None
) -> TS[float]:
    """
    An autoregressive generator.
    The order defines how many terms to use.
    The size of the initial values is ``order`` and coefficients must be the size of ``order+1``.
    """
    result = white_noise.value + coefficients[0]
    prev = _state.previous_terms.value
    result += sum(coefficients[i+1] * prev[i] for i in range(order))
    _state.previous_terms.apply_result((result,) + prev[1:])
    return result

@auto_regressive_generator.start
def autoregressive_generator_start(initial_values: tuple[float, ...], _state: RECORDABLE_STATE[ARState] = None):
    _state.previous_terms.apply_result(initial_values)

