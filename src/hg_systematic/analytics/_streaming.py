from hgraph import TS, INT_OR_TIME_DELTA, operator, compute_node, STATE, CompoundScalar


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

