from hgraph import TS, INT_OR_TIME_DELTA, operator, compute_node, STATE, CompoundScalar
from collections import deque


@operator
def slope_of(
        ts: TS[float], 
        fixed_interval: bool = True,
        window: INT_OR_TIME_DELTA = None
) -> TS[float]:
    """
    Compute the slope of the given time series.
    This should incrementally update the slope with each new value.
    The options include:
    
    fixed_interval: bool = True
        Use time or assume each tick is evenly spaced. The default is
        to assume each tick is evenly spaced.
        
    window: INT_OR_TIME_DELTA = None
        A fixed window of an expanding window, defaults to None, or otherwise
        an expanding window.
    """


class _SlopeState(CompoundScalar):
    n: int = 0
    sum_y: float = 0.0
    sum_iy: float = 0.0


class _RollingSlopeState(CompoundScalar):
    sum_y: float = 0.0
    sum_iy: float = 0.0
    # using deque to store last values; avoid name clash with CompoundScalar.values()
    buf: object = None


class _TimeSlopeState(CompoundScalar):
    n: int = 0
    sum_x: float = 0.0
    sum_y: float = 0.0
    sum_xx: float = 0.0
    sum_xy: float = 0.0


@compute_node(overloads=slope_of, requires=lambda m, s: s.get("fixed_interval") is True and s.get("window") is None)
def slope_of_fixed_interval_no_window(
    ts: TS[float],
    fixed_interval: bool = True,
    window: object = None,
    _state: STATE[_SlopeState] = None,
    _output: TS[float] = None,
) -> TS[float]:
    """
    Incrementally compute slope for equally spaced samples (unit interval) with an expanding window.

    We treat x as the index of the observation: x_i = i for i in [0, n-1].
    Maintain:
      - n = count
      - sum_y = Σ y_i
      - sum_iy = Σ i * y_i
    Then:
      sum_i = n(n-1)/2
      var_x = n(n^2 - 1)/12
      cov_xy = sum_iy - (sum_i * sum_y)/n
      slope = cov_xy / var_x (for n >= 2)
    """
    y = ts.value
    i = _state.n
    _state.n = i + 1
    _state.sum_y += y
    _state.sum_iy += i * y

    n = _state.n
    if n >= 2:
        sum_y = _state.sum_y
        sum_iy = _state.sum_iy
        sum_i = n * (n - 1) / 2.0
        var_x = n * (n * n - 1) / 12.0
        cov_xy = sum_iy - (sum_i * sum_y) / n
        slope = cov_xy / var_x if var_x != 0.0 else 0.0
    else:
        slope = 0.0

    if not _output.valid or _output.value != slope:
        return slope


@compute_node(overloads=slope_of, requires=lambda m, s: s.get("fixed_interval") is True and isinstance(s.get("window"), int))
def slope_of_fixed_interval_fixed_window(
    ts: TS[float],
    fixed_interval: bool = True,
    window: object = None,
    _state: STATE[_RollingSlopeState] = None,
    _output: TS[float] = None,
) -> TS[float]:
    """
    Rolling window slope for equally spaced samples over the last `window` points.

    Efficiently maintains:
      - sum_y = Σ y_i over current window
      - sum_iy = Σ i * y_i where i is 0..n-1 within the window

    On removing the oldest value y0, indices shift down by 1, so:
      sum_iy_new = sum_iy_old - (sum_y_old - y0)
    Then we subtract y0 from sum_y, and when appending y at index n, do:
      sum_iy += n * y; sum_y += y
    """
    # Initialize deque on first use
    if _state.buf is None:
        _state.buf = deque()

    y = ts.value
    w = int(window) if window is not None else None
    if w is None or w <= 0:
        # Degenerate window: treat as emit 0.0 and do nothing
        if not _output.valid or _output.value != 0.0:
            return 0.0
        return

    # If window full, remove oldest and adjust sums
    if len(_state.buf) == w:
        y0 = _state.buf.popleft()
        # adjust sum_iy before updating sum_y since formula uses old sum_y
        _state.sum_iy -= (_state.sum_y - y0)
        _state.sum_y -= y0

    # Append new value
    n_before = len(_state.buf)
    _state.buf.append(y)
    _state.sum_iy += n_before * y
    _state.sum_y += y

    n = len(_state.buf)
    if n >= 2:
        sum_y = _state.sum_y
        sum_iy = _state.sum_iy
        sum_i = n * (n - 1) / 2.0
        var_x = n * (n * n - 1) / 12.0
        cov_xy = sum_iy - (sum_i * sum_y) / n
        slope = cov_xy / var_x if var_x != 0.0 else 0.0
    else:
        slope = 0.0

    if not _output.valid or _output.value != slope:
        return slope


@compute_node(overloads=slope_of, requires=lambda m, s: s.get("fixed_interval") is False and s.get("window") is None)
def slope_of_time_no_window(
    ts: TS[float],
    fixed_interval: bool = False,
    window: object = None,
    _state: STATE[_TimeSlopeState] = None,
    _output: TS[float] = None,
) -> TS[float]:
    """
    Incrementally compute slope for time-sensitive samples with an expanding window.

    Uses ts.last_modified_time as x (converted to seconds since epoch) and performs
    an online linear regression maintaining sufficient statistics:
      - n, sum_x, sum_y, sum_xx, sum_xy
    Slope formula (for n >= 2):
      slope = (n*sum_xy - sum_x*sum_y) / (n*sum_xx - sum_x*sum_x)
    Emits 0.0 when insufficient points or zero variance in time.
    """
    # last_modified_time is a datetime.datetime; convert to POSIX seconds
    t = ts.last_modified_time
    x: float = float(t.timestamp())

    y = ts.value

    _state.n += 1
    _state.sum_x += x
    _state.sum_y += y
    _state.sum_xx += x * x
    _state.sum_xy += x * y

    n = _state.n
    if n >= 2:
        num = n * _state.sum_xy - _state.sum_x * _state.sum_y
        den = n * _state.sum_xx - _state.sum_x * _state.sum_x
        slope = num / den if den != 0.0 else 0.0
    else:
        slope = 0.0

    if not _output.valid or _output.value != slope:
        return slope

