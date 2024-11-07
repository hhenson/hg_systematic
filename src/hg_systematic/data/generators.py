from random import random

import numpy as np
from hgraph import TS, compute_node, \
    RECORDABLE_STATE, TimeSeriesSchema, STATE, SIGNAL, graph

__all__ = ["white_noise_generator", "auto_regressive_generator", "random"]

from numpy.random import PCG64


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
        _state.buffer = np.random.Generator(PCG64()).normal(loc=loc, scale=scale, size=size)
        _state.ndx = 0
    return out


@white_noise_generator.start
def white_noise_generator_start(
        loc: float = 0.0005,
        scale: float = 0.010,
        size: int = 1000,
        _state: STATE = None
):
    _state.buffer = np.random.normal(loc, scale, size=size)
    _state.ndx = 0


@graph
def auto_regressive_generator(
        signal: SIGNAL,
        order: int = 1,
        initial_values: tuple[float, ...] = (1.0,),
        coefficients: tuple[float, ...] = (1.0, 0.5),
        centre: float = 0.0005,
        std_dev: float = 0.010,
        size: int = 1000,
) -> TS[float]:
    """
    An autoregressive generator.  This will generate a sample each time the ``signal`` ticks.
    The generator takes the form of:

    .. math::

        r_{t} = \phi_{0} + \sum_{i=1}^{order} \phi_{i} r_{t-i} + a_{t-1}

    where :math:`r_{t}` is the rate at time t, :math:`\phi_{i}` are the co-efficients and :math:`a_{t-1}` a stream of
    white noise generated using a random normal distribution with centre set to ``centre`` and the standard deviation
    set to ``std_dev``.

    The ``size`` defines the number of samples to pre-generate (ot the white noise).

    The ``order`` is the number of historical value terms in the autoregressive generator.
    """
    noise = white_noise_generator(signal, loc=centre, scale=std_dev, size=size)
    return _auto_regressive_generator(noise, order, initial_values, coefficients)


class ARState(TimeSeriesSchema):
    previous_terms: TS[tuple[float, ...]]


@compute_node(
    requires=lambda m, s: len(s["initial_values"]) == (order := s["order"]) and len(s["coefficients"]) == order + 1
)
def _auto_regressive_generator(
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
    result += sum(coefficients[i + 1] * prev[i] for i in range(order))
    _state.previous_terms.apply_result((result,) + prev[1:])
    return result


@_auto_regressive_generator.start
def autoregressive_generator_start(initial_values: tuple[float, ...], _state: RECORDABLE_STATE[ARState] = None):
    _state.previous_terms.apply_result(initial_values)


@compute_node
def random_(signal: SIGNAL) -> TS[float]:
    """
    Wrap the python random number generator.
    """
    return random()
