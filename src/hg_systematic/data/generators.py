import math
from datetime import timedelta, datetime
from zoneinfo import ZoneInfo

import numpy as np
from hg_oap.dates.dt_utils import date_tz_to_utc
from hgraph import generator, TS, EvaluationEngineApi, graph, lag, delayed_binding, feedback, compute_node, \
    RECORDABLE_STATE, TimeSeriesSchema, STATE, SIGNAL

__all__ = ["white_noise_generator", "auto_regressive_generator"]

@compute_node
def white_noise_generator(
        signal: SIGNAL,
        loc: float = 0.0,
        scale: float = 1.0,
        size: int = 1000,
        _state: STATE = None
) -> TS[float]:
    """
    Generates a stream of white noise at each tick of the ``signal`` input.
    The buffer will be initialised at start, and will be re-initialised if the number of ticks exceeds the buffer
    ``size``.
    The noise stream can now be generated based on any input signal, for example, a calendar ticking out business days.
    """
    out = _state.buffer[_state.ndx]
    _state.ndx += 1
    if size == _state.ndx:
        _state.buffer = np.random.normal(loc, scale, size=size)
        _state.ndx = 0
    return out

@white_noise_generator.start
def white_noise_generator_start(
        loc: float = 0.0,
        scale: float = 1.0,
        size: int = 1000,
        _state: STATE = None
):
    _state.buffer = np.random.normal(loc, scale, size=size)
    _state.ndx = 0



class ARState(TimeSeriesSchema):
    previous_terms: TS[tuple[float,...]]

@compute_node(
    requires=lambda m, s: len(s["initial_values"]) == (order:=s["order"]) and len(s["coefficients"]) == order+1
)
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

