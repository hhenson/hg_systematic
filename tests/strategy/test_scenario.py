from hgraph import operator, TS, graph
from hgraph.test import eval_node

from hg_systematic.strategy.scenario import scenario, default_scenario, scenarios_to_evaluate, reset_scenarios, \
    set_parameters


@operator
def my_op(ts: TS[int]) -> TS[int]:
    ...

@scenario(overloads=my_op, parameters=("extra",))
def my_op_add_one(ts: TS[int], extra: int = 0) -> TS[int]:
    return ts + 1 + extra

@scenario(overloads=my_op)
def my_op_add_two(ts: TS[int]) -> TS[int]:
    return ts + 2

@default_scenario(overloads=my_op)
def my_op_default(ts: TS[int]) -> TS[int]:
    return ts

def test_scenario_decorator():

    @graph
    def g(ts: TS[int]) -> TS[int]:
        return my_op(ts)

    assert eval_node(g, [1, 2, 3]) == [1, 2, 3]

    scenarios_to_evaluate(my_op_add_one)
    assert eval_node(g, [1, 2, 3]) == [2, 3, 4]

    set_parameters("my_op_add_one", extra=1)
    assert eval_node(g, [1, 2, 3]) == [3, 4, 5]

    reset_scenarios()
    scenarios_to_evaluate("my_op_add_two")
    assert eval_node(g, [1, 2, 3]) == [3, 4, 5]

