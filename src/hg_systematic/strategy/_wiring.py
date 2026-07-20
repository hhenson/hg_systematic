"""Implementation-neutral helpers for wrapping hgraph callables."""

from dataclasses import dataclass
from inspect import Parameter, Signature, signature as inspect_signature
from typing import Callable

from hgraph import AUTO_RESOLVE, WiringNodeSignature, graph
from hgraph.reflection import resolved_type


@dataclass(frozen=True)
class CallableSignature:
    name: str
    positional_inputs: dict[str, object]
    keyword_inputs: dict[str, object]
    defaults: dict[str, object]
    output_type: object | None


def as_graph(fn: Callable):
    """Return an hgraph callable, decorating an ordinary function if needed."""
    try:
        wiring_signature = fn.signature
    except AttributeError:
        wiring_signature = None
    return fn if isinstance(wiring_signature, WiringNodeSignature) else graph(fn)


def callable_signature(fn: Callable) -> CallableSignature:
    """Describe a wired callable using ordinary Python annotations."""
    signature = inspect_signature(fn)
    positional_inputs = {}
    keyword_inputs = {}
    defaults = {}

    for name, parameter in signature.parameters.items():
        if parameter.default is AUTO_RESOLVE:
            continue
        annotation = (
            object
            if parameter.annotation is Parameter.empty
            else parameter.annotation
        )
        if parameter.kind in (
            Parameter.POSITIONAL_ONLY,
            Parameter.POSITIONAL_OR_KEYWORD,
        ):
            positional_inputs[name] = annotation
        elif parameter.kind is Parameter.KEYWORD_ONLY:
            keyword_inputs[name] = annotation
        if parameter.default not in (Parameter.empty, None):
            defaults[name] = parameter.default

    output_type = signature.return_annotation
    if output_type is Signature.empty:
        output_type = None

    return CallableSignature(
        name=getattr(fn, "__name__", type(fn).__name__),
        positional_inputs=positional_inputs,
        keyword_inputs=keyword_inputs,
        defaults=defaults,
        output_type=output_type,
    )


def output_type_for(fn: Callable, output=None):
    """Return a resolved public output type for a wired callable or port."""
    wiring_signature = fn.signature
    if output is None and not wiring_signature.unresolved_args:
        try:
            return resolved_type(wiring_signature.output_type)
        except TypeError:
            pass
    if output is None:
        raise TypeError(f"output type for {fn!r} requires wired inputs")
    return resolved_type(output.output_type)
