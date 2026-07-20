from hgraph import TS, SCALAR, GlobalState, IN_MEMORY, get_recorded_value, graph, set_record_replay_model
from hgraph.test import eval_node

from hg_systematic.strategy.recording import recordable, set_recording_prefix, set_record_replay_state, RECORDING_ON, \
    reset_record_replay_state, REPLAYING_ON


@recordable
def my_label_category(ts: TS[int]) -> TS[int]:
    return ts


@recordable(label="generic", category="category")
@graph
def generic_recordable(ts: TS[SCALAR]) -> TS[SCALAR]:
    return ts

def test_recording():

    with GlobalState() as gs:
        set_record_replay_model(IN_MEMORY)
        set_recording_prefix("test")
        set_record_replay_state("my_label", "category", RECORDING_ON)
        assert eval_node(my_label_category, [1, 2, 3]) == [1, 2, 3]
        assert len(get_recorded_value("my_label", "test.category")) == 3

        reset_record_replay_state()
        set_record_replay_state("my_label", "category", REPLAYING_ON)
        assert eval_node(my_label_category, [2, 3, 4]) == [1, 2, 3]


def test_generic_recordable_resolves_its_output_from_wired_inputs():
    with GlobalState():
        set_record_replay_model(IN_MEMORY)
        set_recording_prefix("generic_test")
        set_record_replay_state("generic", "category", RECORDING_ON)
        assert eval_node(generic_recordable, [1, 2]) == [1, 2]

        reset_record_replay_state()
        set_record_replay_state("generic", "category", REPLAYING_ON)
        assert eval_node(generic_recordable, [3, 4]) == [1, 2]
