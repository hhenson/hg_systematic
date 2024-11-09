from datetime import date, datetime

from hg_oap.dates import WeekendCalendar
from hg_oap.dates.date_services import business_days_from_calendar, business_days
from hgraph import TS, feedback, lag, drop_dups, if_cmp, cmp_, sample, graph, register_service, default_path, record, \
    GlobalState, evaluate_graph, GraphConfiguration, set_record_replay_model, sum_, component, RecordReplayContext, \
    RecordReplayEnum
from hgraph.adaptors.data_frame import MemoryDataFrameStorage, DATA_FRAME_RECORD_REPLAY

from hg_systematic.data.generators import auto_regressive_generator

"""
This is an example of how the research code would be converted into code that can be migrated to production.
"""

@graph
def generate_simulated_returns(dt: TS[date]) -> TS[float]:
    return auto_regressive_generator(dt, order=1, initial_values=(0.0,),
                                     coefficients=(0.0, 0.1), centre=0.0, std_dev=0.000211, size=300 * 24)


# We wrap this as a component, this will allow for recording, replay, compare and recovery of the signal.
# At the moment, recovery will not work as we have not implemented the recovery on feedback or lag.
@component(recordable_id="power.signal")
def power_signal(dt: TS[date], returns: TS[float]) -> TS[float]:
    """
    This model is based on the German power auto-correlation strategy proposed by
    [Narro, J., and M. Caamano. Systematic Trading in Energy Markets. London, UK: Risk Books, 2020.]
    Adding the constraint of long only, an end-of-day buy signal is generated using a simple rule: If the difference
    in price from yesterday’s close to today’s is positive, then go long at today’s close, else exit the position.
    .. math::

        y_{t} = \\begin{cases}
                1, & \\quad \\text{if } \\Delta r_{t} > 0 \\\\
                y_{t-1}, & \\quad \\text{if } \\Delta r_{t} = 0 \\\\
                0, & \\quad \\text{if } \\Delta r_{t} < 0
                \\end{cases}

    where :math:`y_{t}` is the side of position, :math:`y_{t} \\in {0, 1}`, and :math:`\\Delta r_{t}` is the difference
    in returns between :math:`r_{t} - r_{t-1}`.
    [Jacques Francois Joubert, Meta-Labeling: Theory and  Framework, 2022]

    :param returns: The returns to compute the strategy on.
    :return: The signal
    """
    ...
    delta_r = returns - lag(returns, period=1)
    fb = feedback(TS[float], 0.0)  # To support the previous signal value as an input to the condition.
    signal_prev = lag(fb(), period=1)
    signal = drop_dups(if_cmp(cmp_(delta_r, 0.0), 0.0, signal_prev, 1.0))
    fb(signal)
    return sample(dt, signal)


@graph
def pnl(returns: TS[float], signal: TS[float]) -> TS[float]:
    """
    Compute the pnl by lagging signal and multiply by returns.
    Finally, perform the sum_ over the series to produce the cumulative pnl.
    """
    pnl = lag(signal, period=1) * returns  # Yesterday's signal * today's return = pnl
    return sum_(pnl)


@graph
def simulate_graph():
    """
    Set up a simulated solution to test strategy.
    """
    register_service(default_path, business_days_from_calendar, calendar_tp=WeekendCalendar)
    dt = business_days()
    returns = generate_simulated_returns(dt)
    signal = power_signal(dt, returns)
    pnl_ = pnl(returns, signal)
    record(pnl_, key="pnl", recordable_id="power_strategy")


if __name__ == '__main__':

    with GlobalState() as gs, MemoryDataFrameStorage() as ds, RecordReplayContext(RecordReplayEnum.RECORD):
        set_record_replay_model(DATA_FRAME_RECORD_REPLAY)
        evaluate_graph(simulate_graph, GraphConfiguration(start_time=datetime(1990, 1, 1),
                                                          end_time=datetime(2020, 1, 1)))
        signal = ds.read_frame("power.signal::__out__")
        print(signal)
        pnl = ds.read_frame("power_strategy::pnl")
        print(pnl)
