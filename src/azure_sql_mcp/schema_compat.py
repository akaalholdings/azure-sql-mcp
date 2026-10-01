"""Portable tool input schemas for strict function-calling clients.

Pydantic emits schemas with ``$ref``/``$defs``, nullable unions
(``anyOf: [{...}, {"type": "null"}]``), ``default``, and per-field ``title``.
Strict validators (Gemini-based clients among them) reject some of these and can
drop the whole tool list. The portable form inlines references, collapses a
nullable union to its single type, moves a default into the description text so
the information survives, and drops titles. Optionality is still expressed by
``required``; server-side validation and defaults are unchanged because they use
the Pydantic argument models, not this advertised schema.
"""

from __future__ import annotations

import copy
import json
from typing import Any

_MAX_REF_DEPTH = 20


def portable_input_schema(schema: dict[str, Any]) -> dict[str, Any]:
    source = copy.deepcopy(schema)
    definitions = {}
    definitions.update(source.pop("definitions", {}) or {})
    definitions.update(source.pop("$defs", {}) or {})
    return _schema(source, definitions, depth=0)


def _schema(node: Any, definitions: dict[str, Any], *, depth: int) -> Any:
    if not isinstance(node, dict):
        return node
    if "$ref" in node and depth < _MAX_REF_DEPTH:
        name = str(node["$ref"]).rsplit("/", 1)[-1]
        target = definitions.get(name)
        if isinstance(target, dict):
            siblings = {key: value for key, value in node.items() if key != "$ref"}
            node = {**copy.deepcopy(target), **siblings}
            depth += 1
    result: dict[str, Any] = {}
    for key, value in node.items():
        if key in {"$defs", "definitions", "title"}:
            continue
        if key == "properties" and isinstance(value, dict):
            result[key] = {
                name: _schema(child, definitions, depth=depth) for name, child in value.items()
            }
        elif key in {"items", "additionalProperties", "not"} and isinstance(value, dict):
            result[key] = _schema(value, definitions, depth=depth)
        elif key in {"anyOf", "oneOf", "allOf", "prefixItems"} and isinstance(value, list):
            result[key] = [_schema(child, definitions, depth=depth) for child in value]
        else:
            result[key] = value
    return _simplify(result)


def _simplify(node: dict[str, Any]) -> dict[str, Any]:
    for key in ("anyOf", "oneOf"):
        options = node.get(key)
        if not isinstance(options, list):
            continue
        non_null = [
            option
            for option in options
            if not (isinstance(option, dict) and option.get("type") == "null")
        ]
        if len(non_null) == 1 and len(non_null) < len(options) and isinstance(non_null[0], dict):
            rest = {name: value for name, value in node.items() if name != key}
            node = {**non_null[0], **rest}
    kind = node.get("type")
    if isinstance(kind, list):
        non_null_types = [item for item in kind if item != "null"]
        if len(non_null_types) == 1:
            node = {**node, "type": non_null_types[0]}
    if "default" in node:
        node = dict(node)
        default = node.pop("default")
        if default is not None:
            text = json.dumps(default, ensure_ascii=True)
            description = str(node.get("description") or "").rstrip()
            node["description"] = f"{description} Default: {text}.".strip()
    return node
