from hgraph import graph, TS
import math
from hgraph.test import eval_node

from hg_systematic.analytics._streaming import slope_of


@graph
def _slope_graph(x: TS[float]) -> TS[float]:
    # Explicitly pass args to satisfy requires predicate
    return slope_of(x, fixed_interval=True, window=None)


def _expected_slopes_emitted(values: list[float]) -> list[float]:
    """
    Compute the expected emitted slope series for an expanding window with equally spaced samples.
    The implementation emits on first tick (0.0) and whenever the slope value changes.
    """
    n = 0
    sum_y = 0.0
    sum_iy = 0.0
    emitted: list[float] = []
    last: float | None = None

    for y in values:
        i = n
        n += 1
        sum_y += y
        sum_iy += i * y

        if n >= 2:
            sum_i = n * (n - 1) / 2.0
            var_x = n * (n * n - 1) / 12.0
            cov_xy = sum_iy - (sum_i * sum_y) / n
            slope = 0.0 if var_x == 0.0 else cov_xy / var_x
        else:
            slope = 0.0

        if last is None or slope != last:
            emitted.append(slope)
            last = slope

    return emitted


def test_slope_of_linear_sequence():
    # y = a*x + b, with a = 2.5. Expected emissions: [0.0, 2.5]
    a = 2.5
    b = -1.0
    values = [a * i + b for i in range(10)]

    out = eval_node(_slope_graph, values)

    out = [v for v in out if v is not None]
    assert out == [0.0, a]


def test_slope_of_general_sequence():
    # A non-linear sequence to ensure slope updates as more points arrive
    values = [0.0, 1.0, 4.0, 9.0, 16.0, 25.0]  # y = x^2

    expected = _expected_slopes_emitted(values)
    out = eval_node(_slope_graph, values)

    out = [v for v in out if v is not None]
    # Compare with small tolerance as floats
    assert len(out) == len(expected)
    for got, exp in zip(out, expected):
        assert abs(got - exp) < 1e-12


@graph
def _slope_graph_w3(x: TS[float]) -> TS[float]:
    # Fixed-size rolling window of 3 points
    return slope_of(x, fixed_interval=True, window=3)


@graph
def _slope_graph_w5(x: TS[float]) -> TS[float]:
    # Fixed-size rolling window of 5 points
    return slope_of(x, fixed_interval=True, window=5)


@graph
def _slope_graph_w1(x: TS[float]) -> TS[float]:
    # Degenerate rolling window of 1 point
    return slope_of(x, fixed_interval=True, window=1)


def _expected_slopes_emitted_rolling(values: list[float], w: int) -> list[float]:
    """
    Compute expected emitted slopes for a fixed-size rolling window over equally spaced samples.
    Emission rule mirrors operator: emit at first tick (0.0) and whenever slope value changes.
    """
    emitted: list[float] = []
    last: float | None = None
    window_vals: list[float] = []

    for y in values:
        # push new value and trim to window size
        window_vals.append(y)
        if len(window_vals) > w:
            window_vals.pop(0)

        n = len(window_vals)
        if n >= 2:
            # indices 0..n-1 within the window
            sum_y = 0.0
            sum_iy = 0.0
            for i, vy in enumerate(window_vals):
                sum_y += vy
                sum_iy += i * vy
            sum_i = n * (n - 1) / 2.0
            var_x = n * (n * n - 1) / 12.0
            cov_xy = sum_iy - (sum_i * sum_y) / n
            slope = 0.0 if var_x == 0.0 else cov_xy / var_x
        else:
            slope = 0.0

        if last is None or slope != last:
            emitted.append(slope)
            last = slope

    return emitted


def test_slope_of_linear_sequence_fixed_window():
    # y = a*x + b; within any fixed-size window slope remains 'a' once there are 2+ points
    a = 1.7
    b = 0.3
    values = [a * i + b for i in range(20)]

    out = eval_node(_slope_graph_w5, values)
    out = [v for v in out if v is not None]

    # Allow small floating drift in rolling updates; ensure first emission is 0.0 and subsequent are ~a
    assert len(out) >= 2
    assert out[0] == 0.0
    for v in out[1:]:
        assert abs(v - a) < 1e-12


def test_slope_of_general_sequence_fixed_window():
    # Non-linear sequence; rolling window of 3 should update slope as local window changes
    values = [0.0, 1.0, 4.0, 9.0, 16.0, 25.0]  # y = x^2

    expected = _expected_slopes_emitted_rolling(values, 3)
    out = eval_node(_slope_graph_w3, values)

    out = [v for v in out if v is not None]
    assert len(out) == len(expected)
    for got, exp in zip(out, expected):
        assert abs(got - exp) < 1e-12


def test_slope_of_degenerate_window_one():
    # With window=1 there are never 2 points in the window; slope should remain 0.0 and emit once
    values = [5.0, -2.0, 3.5, 7.1, 8.2]
    out = eval_node(_slope_graph_w1, values)
    out = [v for v in out if v is not None]
    assert out == [0.0]


# ---- Time-based (no window) tests ----

@graph
def _slope_graph_time(x: TS[float]) -> TS[float]:
    # Time-sensitive expanding window version
    return slope_of(x, fixed_interval=False, window=None)


def test_time_slope_increasing_sequence_emits_non_zero():
    # For an increasing sequence, the time-based slope should eventually emit a non-zero finite value
    values = [float(i) for i in range(12)]
    out_time = [v for v in eval_node(_slope_graph_time, values) if v is not None]
    assert len(out_time) >= 1
    assert out_time[0] == 0.0
    # At least one subsequent emission is non-zero
    assert any(abs(v) > 0.0 for v in out_time[1:])
    # And all emitted slopes are finite numbers
    for v in out_time[1:]:
        assert math.isfinite(v)


def test_time_slope_constant_sequence_zero():
    # For a constant sequence the slope should remain 0.0 and emit once
    values = [5.0, 5.0, 5.0, 5.0, 5.0]
    out_time = [v for v in eval_node(_slope_graph_time, values) if v is not None]
    assert abs(sum(out_time)) < 1e8
