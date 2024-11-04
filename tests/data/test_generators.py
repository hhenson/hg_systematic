from hgraph import MIN_ST, MIN_TD, TS, graph
from hgraph.test import eval_node

from hg_systematic.data.generators import white_noise_generator, auto_regressive_generator, random_


def test_white_noise_generator():
    @graph
    def g(signal: TS[bool]) -> TS[float]:
        return white_noise_generator(signal)
    results = eval_node(g, [True, True, True, True, True])
    print(results)
    assert len(results) == 5


def test_auto_regressive_generator():

    @graph
    def g(signal: TS[bool]) -> TS[float]:
        return auto_regressive_generator(signal)

    results = eval_node(g, [True, True, True, True, True])
    print(results)
    assert len(results) == 5


def test_random():

    @graph
    def g(signal: TS[bool]) -> TS[float]:
        return random_(signal)

    assert len(eval_node(g, [True, True])) == 2
