"""
Provide tools to record / replay components of a strategy. This leverages off of the record/replay logic in HGraph.
"""

__all__ = ["recordable", "set_recording_prefix", "set_record_replay_state", "RECORDING_ON", "REPLAYING_ON",
           "PASS_THROUGH", "reset_record_replay_state", "is_replayable", "is_recording", "set_record_function",
           "set_replay_function", ]

from typing import Callable, Any, TypeVar, Sequence

from hgraph import OUT, WiringNodeClass, graph, with_signature, DebugContext, replay, record
from hgraph.adaptors.data_frame import set_data_frame_overrides

GRAPH_SIGNATURE = TypeVar("GRAPH_SIGNATURE", bound=Callable[..., OUT | None])

_RECORDING_PREFIX = None


def set_recording_prefix(prefix: str):
    """Sets the prefix on the recordable id. This is generally the strategy name."""
    global _RECORDING_PREFIX
    _RECORDING_PREFIX = prefix


RECORDING_ON = True
REPLAYING_ON = False
PASS_THROUGH = None

_RECORD_REPLAY_STATE = None


def set_record_replay_state(label: str = None, category: str = None, state: bool = None):
    """
    Sets the record / replay / ignore state for a given label and category.
    If the label is None, this will set the default state for the category.
    If the category is None, this will set the default state for all labels and categories.
    The state is a ternary boolean, i.e True, False, or None. True sets recording on, False sets replaying on and None
    works as pass-through.
    """
    global _RECORD_REPLAY_STATE
    if _RECORD_REPLAY_STATE is None:
        _RECORD_REPLAY_STATE = {"default": None, "labels": {}, "categories": {}, "pairs": {}}
    if label is None:
        if category is None:
            _RECORD_REPLAY_STATE["default"] = state
        else:
            _RECORD_REPLAY_STATE["categories"][category] = state
    else:
        if category is None:
            _RECORD_REPLAY_STATE["labels"][label] = state
        else:
            _RECORD_REPLAY_STATE["pairs"][f"{label}:{category}"] = state


def reset_record_replay_state():
    """Resets the record / replay / ignore state."""
    global _RECORD_REPLAY_STATE
    _RECORD_REPLAY_STATE = None


def _state_of_record_replay(label: str, category: str) -> bool | None:
    """Returns the state of the record / replay / ignore state for a given label and category."""
    global _RECORD_REPLAY_STATE
    if _RECORD_REPLAY_STATE is None:
        return False
    if (item := f"{label}:{category}") in _RECORD_REPLAY_STATE["pairs"]:
        return _RECORD_REPLAY_STATE["pairs"][item]
    elif label in _RECORD_REPLAY_STATE["labels"]:
        return _RECORD_REPLAY_STATE["labels"][label]
    elif category in _RECORD_REPLAY_STATE["categories"]:
        return _RECORD_REPLAY_STATE["categories"][category]
    else:
        return _RECORD_REPLAY_STATE["default"]


def is_replayable(label: str, category: str) -> bool:
    """True if this label and category is replayable."""
    state = _state_of_record_replay(label, category)
    return state is REPLAYING_ON


def is_recording(label: str, category: str) -> bool:
    """True if this label and category is to be recorded."""
    state = _state_of_record_replay(label, category)
    return state is RECORDING_ON


_RECORD_FUNCTION = None
_REPLAY_FUNCTION = None


def set_record_function(fn: Callable):
    """Sets the record function to be used. If not set, the default hgraph.record will be used."""
    global _RECORD_FUNCTION
    _RECORD_FUNCTION = fn


def set_replay_function(fn: Callable):
    """Sets the replay function to be used. if not set, the default hgraph.replay will be used."""
    global _REPLAY_FUNCTION
    _REPLAY_FUNCTION = fn


def get_record_function():
    return _RECORD_FUNCTION if _RECORD_FUNCTION is not None else record


def get_replay_function():
    return _REPLAY_FUNCTION if _REPLAY_FUNCTION is not None else replay


def recordable(
        fn: GRAPH_SIGNATURE = None, *, label: str = None, category: str = None, overloads: Callable = None,
        track_as_of: bool = None, track_removes: bool = None, partition_keys: Sequence[str] = None,
        remove_partition_keys: Sequence[str] = None
) -> GRAPH_SIGNATURE:
    """
    Wraps a graph so that it can be recorded or replayed. This will make use of configuration to determine if the
    graph should be recorded, replayed, or neither.

    If no label, but a category is provided, then the label will be the function name, if neither label, nor category
    are provided, and the function contains at least one _, the last part (when split by _) will be used as the
    category and the prior part as the label.

    To make use of recordable, the record / replay constraints must be met in the outer wiring and configuration.

    For example:

    @recordable(category="signal")
    def my_signal() -> TSD[str, TS[float]]:
        ...

    @graph
    def strategy():
        ...
        debug_print(my_signal(), sample=100)

    if __name__ == "__main__":
        with GlobalState() as gs, FileBasedDataFrameStorage(Path.home() / "tmp") as ds:
            set_as_of(datetime.utcnow())
            set_record_replay_model(DATA_FRAME_RECORD_REPLAY)
            set_recording_prefix("my_strategy")
            set_table_schema_as_of_key("as_of")
            set_table_schema_date_key("date")
            evaluate_graph(strategy, GraphConfiguration(start_time=datetime(2000, 1, 1), end_time=datetime(2020, 1, 1)))

    NOTE: The track_as_of, track_removes, partition_keys, and remove_partition_keys are only used if the recordable is
    evaluated using a DATA_FRAME_RECORD_REPLAY record_replay_model.

    :param fn: The function to wrap.
    :param label: The unique label within the category provided.
    :param category: The category of the logic.
    :param overloads: Support to implement graph overloads using recordable tag.
    :param track_as_of: Whether to track the as_of value for the recordable.
    :param track_removes: Whether to track removes for the recordable.
    :param partition_keys: The partition keys for the recordable.
    :param remove_partition_keys: The remove partition keys for the recordable.
    :return: A graph component.
    """

    if fn is None:
        # If no fn is provided, then we are using this as a decorator and need to capture the fn in the second call
        return lambda fn: recordable(fn, label=label, category=category, overloads=overloads)

    if not isinstance(fn, WiringNodeClass):
        # The fn is not a graph or node instance so wrap it in a graph.
        fn = graph(fn)

    signature = fn.signature  # Since this is a WiringNodeClass now
    if no_label := label is None:
        label = signature.name
    if category is None and "_" in signature.name:
        lbl, category = signature.name.rsplit("_", 1)
        if no_label:
            label = lbl

    if category is None:
        raise ValueError("Must provide a category or label for recordable (or include '_' in the function name).")

    non_auto_resolve = signature.non_autoresolve_inputs
    pos_inputs = {k: v for k, v in signature.positional_inputs.items() if k in non_auto_resolve}
    kw_inputs = {k: v for k, v in signature.kw_only_inputs.items() if k in non_auto_resolve}
    defaults = {k: v for k, v in signature.defaults.items() if k in non_auto_resolve and v is not None}

    @with_signature(
        args=pos_inputs,
        kwargs=kw_inputs,
        defaults=defaults,
        return_annotation=signature.output_type
    )
    def wrapper(*args, **kwargs):
        recordable_id = f"{_RECORDING_PREFIX}.{category}"
        # Set the data frame overrides to make use of for this recordable data item.
        set_data_frame_overrides(key=label, recordable_id=recordable_id, track_as_of=track_as_of,
                                 track_removes=track_removes, partition_keys=partition_keys,
                                 remove_partition_keys=remove_partition_keys)
        if is_replayable(label, category):
            if not signature.output_type.is_resolved:
                # In this case we must first attempt to resolve the output type.
                out = fn(*args, **kwargs)
                tp = out.output_type
                if not tp.is_resolved:
                    raise ValueError(f"Cannot record output type {tp} for {fn} as it is not resolved.")
            else:
                tp = signature.output_type
            if DebugContext.instance() and DebugContext.instance().debug:
                print(f"Replaying {recordable_id}.{label} with type: {tp}")
            # Replay the data, this will cause the fn to be wired out of the graph (and any nodes solely dependent on it
            return get_replay_function()(key=label, tp=tp.py_type, recordable_id=recordable_id)
        else:
            out = fn(*args, **kwargs)
            if is_recording(label, category):
                if DebugContext.instance() and DebugContext.instance().debug:
                    print(f"Recording {recordable_id}.{label} with type: {out.output_type}")
                get_record_function()(out, key=label, recordable_id=recordable_id)
            return out

    wrapper.__name__ = signature.name
    wrapper.__doc__ = fn.fn.__doc__
    wrapper = graph(wrapper, overloads=overloads)

    return wrapper
