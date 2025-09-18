from hgraph import TS, GlobalState, set_record_replay_model, IN_MEMORY, get_recorded_value
from hgraph.test import eval_node

from hg_systematic.strategy.recording import recordable, set_recording_prefix, set_record_replay_state, RECORDING_ON, \
    reset_record_replay_state, REPLAYING_ON


@recordable
def my_label_category(ts: TS[int]) -> TS[int]:
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

