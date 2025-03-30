Formula and their conversion to HGraph
======================================

For the conversion of formulas to HGraph, we use the following definitions:

:math:`t` is a time-series of ``TIME_TYPE`` values, which can be either ``date`` or ``datetime``. This is expected
to tick with a constant frequency with respect to values being processed. For example when we are pricing this should
tick with priceable days. The expected use of this to indicate current value or previous values in the form of:

* previous price: :math:`p_{t-1}`

* current price: :math:`p_{t}`.

This works with a ``lag`` operator where the :math:`- i` represents how many ticks of the ``TIME_TYPE`` time-series
to delay the value by. For example, ``lag(price, i, t)``.


Simple Return
-------------

.. math::

    r = \frac{p_t}{p_{t-1}} - 1

::

    TIME_TYPE = TypeVar("TIME_TYPE", date, datetime)

    @graph
    def simple_return(p: TS[float], t: TS[TIME_TYPE]) -> TS[float]:
        return (p / lag(p, 1, t) - 1.0

