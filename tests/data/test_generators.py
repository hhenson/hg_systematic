from hgraph import MIN_ST, MIN_TD
from hgraph.test import eval_node

from hg_systematic.data.generators import white_noise_generator


def test_white_noise_generator():

    results = eval_node(white_noise_generator, frequency=MIN_TD, __end_time__=MIN_ST + MIN_TD*5)
    print(results)
    assert len(results) == 5
