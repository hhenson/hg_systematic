from hgraph import graph, TS
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
