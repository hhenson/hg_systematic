"""
Scenarios allow for parameterised testing of strategies, where an operator can be defined to describe a step or element
of a strategy, but where different approaches can be taken to achieve the objective. This allows for creating different
runs / scenarios to be evaluated without losing the overall shape of the strategy.
"""
from typing import Callable, Sequence

__all__ = ["scenario", "scenarios_to_evaluate", "reset_scenarios", "is_scenario_active", "use_default_scenario",
           "register_scenario", "set_parameters", "get_active_parameters", "default_scenario",]

from hgraph import WiringNodeClass, graph, with_signature

_SCENARIOS = {}
_LBL_TO_OVERLOAD = {}
_ACTIVE_SCENARIOS = set()
_ACTIVE_PARAMETERS = {}


def scenarios_to_evaluate(*labels: str | Callable):
    """Sets the active scenarios for the next evaluation."""
    global _ACTIVE_SCENARIOS
    _ACTIVE_SCENARIOS.update(l if isinstance(l, str) else l.signature.name for l in labels)


def reset_scenarios():
    """Resets the active scenarios."""
    global _ACTIVE_SCENARIOS, _ACTIVE_PARAMETERS
    _ACTIVE_SCENARIOS = set()
    _ACTIVE_PARAMETERS = {}


def is_scenario_active(label: str) -> bool | str:
    """Returns true if the scenario is active."""
    global _ACTIVE_SCENARIOS
    return label in _ACTIVE_SCENARIOS or f"{label} not active"


def use_default_scenario(overloads: Callable) -> bool:
    """Indicates if the default scenario should be used."""
    global _SCENARIOS, _ACTIVE_SCENARIOS
    keys = set(_SCENARIOS.get(overloads, {}).keys())
    return len(k := keys.intersection(_ACTIVE_SCENARIOS)) == 0 or f"{k} active so default should not be used"


def register_scenario(label: str, overloads: Callable = None, parameters: Sequence[str] = None):
    """Registers a scenario."""
    global _SCENARIOS, _LBL_TO_OVERLOAD
    if label in _LBL_TO_OVERLOAD and overloads in _LBL_TO_OVERLOAD[label]:
        raise ValueError(f"Scenario label {label} already registered for overload: {overloads.signature.signature}.")
    _LBL_TO_OVERLOAD.setdefault(label, set()).add(overloads)
    _SCENARIOS.setdefault(overloads, {})[label] = parameters or []


def set_parameters(label: str | Callable, **kwargs):
    """Sets the parameters for a scenario."""
    global _ACTIVE_PARAMETERS, _LBL_TO_OVERLOAD, _SCENARIOS
    if not isinstance(label, str):
        label = label.signature.name
    overloads = _LBL_TO_OVERLOAD[label]
    assert len(overloads) >= 1, f"Expected at least 1 overload for scenario label {label}"
    # All overloads using the same scenario label must use the same parameters,
    # so just pick one and run with it
    overload = next(iter(overloads))
    parameters = _SCENARIOS[overload][label]
    if len(parameters) != len(kwargs) or set(kwargs.keys()) - set(parameters):
        # We must set parameters defined for the scenario only, but we can miss some out (this assumes there is a default)
        raise ValueError(f"Expected {parameters} parameters, got {kwargs.keys()}.")
    _ACTIVE_PARAMETERS[label] = kwargs


def get_active_parameters(label: str):
    """Gets the parameters for a scenario."""
    global _ACTIVE_PARAMETERS
    return _ACTIVE_PARAMETERS.get(label, {})


def get_all_active_parameters():
    """The current active parameters."""
    global _ACTIVE_PARAMETERS
    return _ACTIVE_PARAMETERS


def get_active_scenarios():
    """The active scenarios."""
    global _ACTIVE_SCENARIOS
    return _ACTIVE_SCENARIOS


def scenario(fn=None, *, label: str = None, overloads: Callable = None, parameters: Sequence[str] = None):
    """
    Wraps a graph (or node) with a scenario label and associates the wrapped function with the overload provided.

    When the label is set as a run-time scenario, then the scenario will be used as the implementation of the overload.
    This uses the standard hgraph.operator logic. Thus, the rules for the operator decorator apply.

    Use the use_scenarios function to set this scenario as the scenario to evaluate.

    :param label: The label of the scenario, if not provided, the function name is used.
    :param overloads: The generic operator this scenario is implementing.
    :param parameters: Any parameters that are configurable for this scenario.
    """
    if fn is None:
        return lambda fn: scenario(fn, label=label, overloads=overloads, parameters=parameters)

    if not isinstance(fn, WiringNodeClass):
        # The fn is not a graph or node instance so wrap it in a graph.
        fn = graph(fn)

    signature = fn.signature
    if label is None:
        label = signature.name

    non_auto_resolve = fn.signature.non_autoresolve_inputs
    pos_inputs = {k: v for k, v in fn.signature.positional_inputs.items() if k in non_auto_resolve}
    kw_inputs = {k: v for k, v in fn.signature.kw_only_inputs.items() if k in non_auto_resolve}
    defaults = {k: v for k, v in fn.signature.defaults.items() if k in non_auto_resolve and v is not None}

    @with_signature(
        args=pos_inputs,
        kwargs=kw_inputs,
        defaults=defaults,
        return_annotation=fn.signature.output_type
    )
    def wrapper(*args, **kwargs):
        parameters = get_active_parameters(label)
        kwargs.update(parameters)
        return fn(*args, **kwargs)

    wrapper.__name__ = signature.name
    wrapper.__doc__ = fn.fn.__doc__
    wrapper = graph(wrapper, overloads=overloads, requires=lambda m, s, l=label: is_scenario_active(l))

    register_scenario(label, overloads, parameters)

    return wrapper

def default_scenario(fn=None, *, overloads: Callable = None):
    """
    What to do if no scenario is set.
    :param overloads: The generic operator this default scenario is implementing.
    """
    if fn is None:
        return lambda fn: default_scenario(fn, overloads=overloads)

    if not isinstance(fn, WiringNodeClass):
        # The fn is not a graph or node instance so wrap it in a graph.
        fn = graph(fn)

    signature = fn.signature

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
        return fn(*args, **kwargs)

    wrapper.__name__ = signature.name
    wrapper.__doc__ = fn.fn.__doc__
    wrapper = graph(wrapper, overloads=overloads, requires=lambda m, s, o=overloads: use_default_scenario(o))

    return wrapper