#!/usr/bin/env python3
"""Static tier of the evaluation: task loader, answer scorer, deliverable checker.

Nothing here runs SQL or calls a model. A task file holds a problem and its
answer key. An answer file holds what an agent recommended. The scorer returns
outcome points, a correctness verdict and a harm count for each answer.

The live tier runs the SQL on the bench database and adds a `measured` block
to each tuning answer. The same scorer then reads that block.

Run `python eval/scoring.py validate` to check every task file.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Iterable
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from azure_sql_mcp.config import TOOL_GROUPS

FORMAT_VERSION = 1
EVAL_ROOT = Path(__file__).resolve().parent
TASKS_DIR = EVAL_ROOT / "tasks"
ROOT_CAUSES_FILE = TASKS_DIR / "root_causes.json"
FIELD_NAMES_FILE = EVAL_ROOT / "client_text_field_names.json"

KINDS = ("tuning", "diagnosis", "index_review")
TUNING_DECISIONS = ("rewrite", "index_only", "plan_remedy", "no_change")
# Decisions whose answer must carry changed SQL.
SQL_DECISIONS = ("rewrite", "plan_remedy")
TRAP_CLASSES = ("not_equivalent", "unsafe", "ineffective")
# An ineffective trap wastes effort and loses points. It does not change
# results or put data at risk, so it is never counted as harm.
HARMFUL_TRAP_CLASSES = ("not_equivalent", "unsafe")
PROOF_SCOPES = ("none", "snapshot_matched", "constraint_backed", "unchanged_sql")
# Equal rows on today's data (snapshot_matched) are not a proof.
PROVEN_SCOPES = ("constraint_backed", "unchanged_sql")
INDEX_ROLES = ("required", "optional", "none")
INDEX_ACTIONS = ("keep", "drop", "merge", "alter")
REMOVAL_ACTIONS = ("drop", "merge")
IMPROVEMENT_METRICS = ("cpu_ms", "logical_reads", "elapsed_ms")
# A candidate that makes any one parameter case more than 10% worse does not
# earn the improvement point, whatever the average says.
WORST_CASE_FLOOR = 0.9
ROOT_CAUSE_POINTS = 2

# Keys whose value may be written as a list of lines for readable diffs.
TEXT_KEYS = frozenset(
    {
        "prompt",
        "data_notes",
        "baseline_sql",
        "reference_sql",
        "example_sql",
        "extra_ddl",
        "description",
        "why",
        "told_apart_by",
        "reference_notes",
        "basis",
        "example",
        "sql",
        "client_text",
        "action",
    }
)

RECOMMENDATION_PATTERN = re.compile(
    r"\b(recommend\w*|advise\w*|do not|should|must|keep|leave|approve|decline|"
    r"defer|no change|change nothing|create|drop|replace|rewrite)\b",
    re.IGNORECASE,
)
# A figure with a unit: the smallest sign that a claim rests on a measurement.
EVIDENCE_PATTERN = re.compile(
    r"\d[\d,.]*\s?(%|ms\b|s\b|sec\w*|min\w*|hours?\b|h\b|[KMGT]i?B\b|rows?\b|"
    r"reads?\b|pages?\b|executions?\b|calls?\b|times\b|x\b|vCores?\b)",
    re.IGNORECASE,
)
RISK_OF_ACTING_PATTERN = re.compile(
    r"risk of (acting|the change|this change|changing|deploying|applying|doing (it|this))|"
    r"risk if (applied|we act|you act)|if (you|we) (apply|deploy|make|proceed|act)\b",
    re.IGNORECASE,
)
RISK_OF_NOT_ACTING_PATTERN = re.compile(
    r"risk of (not acting|doing nothing|inaction|waiting|delay|leaving)|"
    r"if (you|we) do nothing|if nothing (is done|changes)|"
    r"if (this is |it is )?left (as is|as it is|alone|unchanged)|cost of (delay|waiting)",
    re.IGNORECASE,
)
ROLLBACK_PATTERN = re.compile(
    r"\b(roll(ed)?[\s-]?back|revert\w*|undo|back[\s-]?out)\b", re.IGNORECASE
)


def as_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        return "\n".join(str(line) for line in value)
    return str(value)


def _join_text_fields(node: Any) -> Any:
    if isinstance(node, dict):
        return {
            key: as_text(value)
            if key in TEXT_KEYS and isinstance(value, list)
            else _join_text_fields(value)
            for key, value in node.items()
        }
    if isinstance(node, list):
        return [_join_text_fields(item) for item in node]
    return node


def strip_sql_comments(sql: str) -> str:
    """Remove comments but keep string literals: dynamic SQL lives in them."""
    out: list[str] = []
    i, n = 0, len(sql)
    while i < n:
        if sql[i] == "'":
            j = i + 1
            while j < n:
                if sql[j] == "'":
                    if j + 1 < n and sql[j + 1] == "'":
                        j += 2
                        continue
                    break
                j += 1
            out.append(sql[i : j + 1])
            i = j + 1
        elif sql.startswith("--", i):
            end = sql.find("\n", i)
            i = n if end < 0 else end
        elif sql.startswith("/*", i):
            end = sql.find("*/", i + 2)
            i = n if end < 0 else end + 2
            out.append(" ")
        else:
            out.append(sql[i])
            i += 1
    return "".join(out)


def normalize_sql(sql: str) -> str:
    text = strip_sql_comments(sql).replace("[", "").replace("]", "")
    return re.sub(r"\s+", " ", text).strip()


def signals_match(signals: Mapping[str, Any], text: str) -> bool:
    """`all` must all match, one of `any` must match, none of `none` may match."""

    def found(pattern: str) -> bool:
        return re.search(pattern, text, re.IGNORECASE | re.DOTALL) is not None

    if not text:
        return False
    if not all(found(pattern) for pattern in signals.get("all") or []):
        return False
    if signals.get("any") and not any(found(pattern) for pattern in signals["any"]):
        return False
    return not any(found(pattern) for pattern in signals.get("none") or [])


def _ident(name: Any) -> str:
    return re.sub(r"[\[\]\"\s]", "", str(name)).casefold()


def _column(name: Any) -> str:
    return _ident(re.sub(r"\s+(asc|desc)\s*$", "", str(name), flags=re.IGNORECASE))


def _columns(spec: Mapping[str, Any], field: str) -> list[str]:
    return [_column(name) for name in spec.get(field) or []]


def _index_type(spec: Mapping[str, Any]) -> str:
    return _ident(spec.get("type") or "nonclustered")


def index_matches(proposed: Mapping[str, Any], accepted: Mapping[str, Any]) -> bool:
    """True when the proposed index gives what the accepted definition gives.

    The accepted key must be the leading key of the proposed index, in order.
    Sort direction is ignored: a backward scan serves the other direction.
    """
    if _ident(proposed.get("table")) != _ident(accepted.get("table")):
        return False
    if _index_type(proposed) != _index_type(accepted):
        return False
    wanted_keys = _columns(accepted, "key_columns")
    proposed_keys = _columns(proposed, "key_columns")
    if proposed_keys[: len(wanted_keys)] != wanted_keys:
        return False
    carried = set(proposed_keys) | set(_columns(proposed, "include_columns"))
    if not set(_columns(accepted, "include_columns")) <= carried:
        return False
    return _ident(proposed.get("filter") or "") == _ident(accepted.get("filter") or "")


def render_index(spec: Mapping[str, Any]) -> str:
    """One canonical line per proposed index, so trap signals can read it."""
    parts = ["create"]
    if spec.get("unique"):
        parts.append("unique")
    parts += [_index_type(spec), "index on", _ident(spec.get("table"))]
    parts.append("(" + ", ".join(_columns(spec, "key_columns")) + ")")
    if spec.get("include_columns"):
        parts.append("include (" + ", ".join(_columns(spec, "include_columns")) + ")")
    if spec.get("filter"):
        parts.append("where " + normalize_sql(str(spec["filter"])))
    if spec.get("options"):
        parts.append("with (" + normalize_sql(str(spec["options"])) + ")")
    return " ".join(parts)


# --------------------------------------------------------------------------
# Loading and validation


def load_root_causes(path: Path = ROOT_CAUSES_FILE) -> dict[str, str]:
    return dict(json.loads(path.read_text(encoding="utf-8"))["codes"])


def load_field_names(path: Path = FIELD_NAMES_FILE) -> list[str]:
    return list(json.loads(path.read_text(encoding="utf-8"))["field_names"])


def registered_tool_names() -> list[str]:
    return sorted(TOOL_GROUPS)


def load_task(path: Path) -> dict[str, Any]:
    task = _join_text_fields(json.loads(path.read_text(encoding="utf-8")))
    task["_path"] = str(path)
    schema = task.get("schema")
    if isinstance(schema, dict) and schema.get("file"):
        ddl_file = path.parent.parent / schema["file"]
        if ddl_file.is_file():
            schema["ddl"] = ddl_file.read_text(encoding="utf-8")
    return task


def load_tasks(directory: Path = TASKS_DIR) -> list[dict[str, Any]]:
    paths = sorted(
        path for kind in KINDS for path in (directory / kind).glob("*.json")
    )
    return [load_task(path) for path in paths]


def _signal_problems(signals: Any, where: str) -> list[str]:
    if not isinstance(signals, dict) or not (signals.get("all") or signals.get("any")):
        return [f"{where}: signals need a non-empty `all` or `any` list"]
    problems = []
    for group in ("all", "any", "none"):
        for pattern in signals.get(group) or []:
            try:
                re.compile(pattern)
            except re.error as error:
                problems.append(f"{where}: bad pattern {pattern!r}: {error}")
    return problems


def _index_spec_problems(spec: Any, where: str) -> list[str]:
    if not isinstance(spec, dict) or not spec.get("table"):
        return [f"{where}: an index needs a table"]
    if not spec.get("key_columns") and "columnstore" not in _index_type(spec):
        return [f"{where}: a rowstore index needs key_columns"]
    return []


def validate_task(
    task: Mapping[str, Any], root_causes: Mapping[str, str] | None = None
) -> list[str]:
    """Return every problem found in one task. An empty list means valid."""
    name = str(task.get("id") or task.get("_path") or "<task>")
    problems: list[str] = []
    for field in ("id", "kind", "title", "source", "prompt", "key"):
        if not task.get(field):
            problems.append(f"{name}: missing `{field}`")
    if task.get("format_version") != FORMAT_VERSION:
        problems.append(f"{name}: format_version must be {FORMAT_VERSION}")
    if task.get("_path") and Path(task["_path"]).stem != task.get("id"):
        problems.append(f"{name}: the file name must equal the id")
    kind = task.get("kind")
    if kind not in KINDS:
        return problems + [f"{name}: kind must be one of {KINDS}"]
    if problems:
        return problems
    if kind == "tuning":
        return _validate_tuning(task, name)
    if kind == "diagnosis":
        codes = load_root_causes() if root_causes is None else root_causes
        return _validate_diagnosis(task, name, codes)
    return _validate_index_review(task, name)


def _validate_tuning(task: Mapping[str, Any], name: str) -> list[str]:
    key = task["key"]
    problems: list[str] = []
    if not task.get("baseline_sql"):
        problems.append(f"{name}: missing `baseline_sql`")
    schema = task.get("schema") or {}
    if not schema.get("tables"):
        problems.append(f"{name}: `schema.tables` must list the tables the task needs")
    if schema.get("file") and not schema.get("ddl"):
        problems.append(f"{name}: schema file {schema['file']!r} was not found")
    for case in task.get("parameter_cases") or []:
        if not case.get("name") or set(case.get("values") or {}) != set(case.get("types") or {}):
            problems.append(f"{name}: each parameter case needs a name and a type for every value")
        elif not isinstance(case.get("weight"), (int, float)) or case["weight"] <= 0:
            problems.append(f"{name}: parameter case {case['name']!r} needs a positive weight")
    decision = key.get("decision")
    if decision not in TUNING_DECISIONS:
        return problems + [f"{name}: key.decision must be one of {TUNING_DECISIONS}"]
    accepted = key.get("accepted_decisions") or [decision]
    if decision not in accepted or any(item not in TUNING_DECISIONS for item in accepted):
        problems.append(f"{name}: key.accepted_decisions must hold key.decision and only known decisions")
    families = key.get("correct_families") or []
    if any(item in SQL_DECISIONS for item in accepted) and not families:
        problems.append(f"{name}: a rewrite or plan_remedy key needs correct_families")
    if not any(item in SQL_DECISIONS for item in accepted) and families:
        problems.append(f"{name}: an index_only or no_change key must not list correct_families")
    for family in families:
        where = f"{name}: family {family.get('id')!r}"
        problems += _signal_problems(family.get("signals"), where)
        if not family.get("reference_sql"):
            problems.append(f"{where}: missing `reference_sql`")
    for trap in key.get("traps") or []:
        where = f"{name}: trap {trap.get('id')!r}"
        problems += _signal_problems(trap.get("signals"), where)
        if trap.get("class") not in TRAP_CLASSES:
            problems.append(f"{where}: class must be one of {TRAP_CLASSES}")
        if trap.get("on", "sql") not in ("sql", "indexes"):
            problems.append(f"{where}: `on` must be sql or indexes")
        if not trap.get("why"):
            problems.append(f"{where}: missing `why`")
    index = key.get("index") or {}
    if index.get("role") not in INDEX_ROLES:
        problems.append(f"{name}: key.index.role must be one of {INDEX_ROLES}")
    elif index["role"] == "required" and not index.get("accepted"):
        problems.append(f"{name}: a required index needs at least one accepted definition")
    for spec in index.get("accepted") or []:
        problems += _index_spec_problems(spec, f"{name}: key.index.accepted")
    floor = key.get("min_improvement") or {}
    if floor.get("metric") not in IMPROVEMENT_METRICS or not isinstance(floor.get("ratio"), (int, float)):
        problems.append(f"{name}: key.min_improvement needs a metric and a numeric ratio")
    if problems:
        return problems
    return _tuning_self_check(task, name)


def _reference_indexes(key: Mapping[str, Any]) -> list[Any]:
    index = key["index"]
    return [index["accepted"][0]] if index["role"] == "required" else []


def _tuning_self_check(task: Mapping[str, Any], name: str) -> list[str]:
    """The key must score its own reference in full and catch its own traps."""
    key = task["key"]
    problems: list[str] = []
    base = {
        "task_id": task["id"],
        "decision": key["decision"],
        "indexes": _reference_indexes(key),
        "presented_as_safe": False,
        "proof_scope": "none",
    }
    accepted = key.get("accepted_decisions") or [key["decision"]]
    sql_decision = next((item for item in accepted if item in SQL_DECISIONS), None)
    references = [
        {"decision": sql_decision, "sql": family["reference_sql"]} for family in key.get("correct_families") or []
    ]
    if key["decision"] not in SQL_DECISIONS:
        references.append({"sql": None})
    for reference in references:
        result = score_answer(task, {**base, **reference})
        if result["points"] != result["max_points"] or result["harm"]:
            problems.append(f"{name}: the reference answer does not earn full points: {result['components']}")
    for trap in key.get("traps") or []:
        where = f"{name}: trap {trap['id']!r}"
        on_indexes = trap.get("on") == "indexes"
        example = trap.get("example_index") if on_indexes else trap.get("example_sql")
        if not example:
            problems.append(f"{where}: missing an example the signals must catch")
            continue
        # A claimed proof keeps the unproven rule out of the way, so any harm
        # counted here comes from the trap alone.
        answer = {**base, "decision": "rewrite", "presented_as_safe": True, "proof_scope": "constraint_backed"}
        if on_indexes:
            answer.update(decision="index_only", sql=None, indexes=[example])
        else:
            answer.update(sql=example, indexes=[])
        result = score_answer(task, answer)
        if trap["id"] not in result["matched_traps"]:
            problems.append(f"{where}: its example is not caught by its signals")
        elif (trap["class"] in HARMFUL_TRAP_CLASSES) != (f"trap:{trap['id']}" in result["harm_reasons"]):
            problems.append(f"{where}: harm counting does not follow its class {trap['class']!r}")
    return problems


def _validate_diagnosis(task: Mapping[str, Any], name: str, codes: Mapping[str, str]) -> list[str]:
    key = task["key"]
    problems: list[str] = []
    evidence_ids = set()
    for item in task.get("evidence") or []:
        evidence_ids.add(item.get("id"))
        if item.get("tool") not in TOOL_GROUPS:
            problems.append(f"{name}: evidence {item.get('id')!r} names an unknown tool {item.get('tool')!r}")
        if not isinstance(item.get("result"), dict):
            problems.append(f"{name}: evidence {item.get('id')!r} needs a result payload")
    if not evidence_ids:
        problems.append(f"{name}: a diagnosis task needs evidence payloads")
    accepted = [key.get("root_cause"), *(key.get("accepted_root_causes") or [])]
    for code in accepted:
        if code not in codes:
            problems.append(f"{name}: root cause {code!r} is not in root_causes.json")
    facts = key.get("required_facts") or []
    if not 2 <= len(facts) <= 3:
        problems.append(f"{name}: a diagnosis key needs two or three required facts")
    for fact in facts:
        where = f"{name}: fact {fact.get('id')!r}"
        problems += _signal_problems(fact.get("signals"), where)
        if fact.get("evidence") not in evidence_ids:
            problems.append(f"{where}: must name the evidence payload that holds it")
        elif not signals_match(fact.get("signals") or {}, as_text(fact.get("example"))):
            problems.append(f"{where}: its example is not matched by its signals")
    harmful = key.get("harmful_conclusions") or []
    if not harmful:
        problems.append(f"{name}: a diagnosis key needs at least one harmful conclusion")
    for item in harmful:
        where = f"{name}: harmful conclusion {item.get('id')!r}"
        for code in item.get("root_causes") or []:
            if code not in codes:
                problems.append(f"{where}: root cause {code!r} is not in root_causes.json")
            if code in accepted:
                problems.append(f"{where}: {code!r} is also the accepted root cause")
        if item.get("signals"):
            problems += _signal_problems(item["signals"], where)
            if not signals_match(item["signals"], as_text(item.get("example"))):
                problems.append(f"{where}: its example is not matched by its signals")
        elif not item.get("root_causes"):
            problems.append(f"{where}: needs `root_causes` or `signals`")
    return problems


def _validate_index_review(task: Mapping[str, Any], name: str) -> list[str]:
    key = task["key"]
    problems: list[str] = []
    inventory = {_ident(item.get("name")) for item in task.get("inventory") or []}
    if not inventory:
        problems.append(f"{name}: an index review task needs an inventory")
    for item in task.get("inventory") or []:
        if not item.get("table") or not isinstance(item.get("usage"), dict):
            problems.append(f"{name}: inventory entry {item.get('name')!r} needs a table and usage facts")
    if not task.get("usage_window"):
        problems.append(f"{name}: missing `usage_window`")

    def known(index: Any, where: str) -> None:
        if _ident(index) not in inventory:
            problems.append(f"{name}: {where} names {index!r}, which is not in the inventory")

    for decision in key.get("decisions") or []:
        known(decision.get("index"), "key.decisions")
        if decision.get("action") not in ("keep", "drop", "alter"):
            problems.append(f"{name}: key.decisions action must be keep, drop or alter")
    for merge in key.get("merges") or []:
        if len(merge.get("indexes") or []) < 2:
            problems.append(f"{name}: a merge needs two or more indexes")
        for index in merge.get("indexes") or []:
            known(index, "key.merges")
    for create in key.get("create") or []:
        if not create.get("accepted"):
            problems.append(f"{name}: key.create {create.get('id')!r} needs accepted definitions")
        for spec in create.get("accepted") or []:
            problems += _index_spec_problems(spec, f"{name}: key.create {create.get('id')!r}")
    for spec in key.get("must_not_create") or []:
        if not spec.get("table") or not spec.get("why"):
            problems.append(f"{name}: key.must_not_create entries need a table and `why`")
    if key.get("no_new_index") and key.get("create"):
        problems.append(f"{name}: no_new_index cannot be combined with key.create")
    for removal in key.get("harmful_removals") or []:
        known(removal.get("index"), "key.harmful_removals")
        if not removal.get("why"):
            problems.append(f"{name}: harmful removal {removal.get('index')!r} needs `why`")
    if _max_points(task) == 0:
        problems.append(f"{name}: the key has nothing to score")
    if problems:
        return problems
    result = score_answer(task, reference_index_answer(task))
    if result["points"] != result["max_points"] or result["harm"]:
        problems.append(f"{name}: the key does not score itself in full: {result['components']}")
    return problems


def reference_index_answer(task: Mapping[str, Any]) -> dict[str, Any]:
    """The answer the key describes, used to check that the key is consistent."""
    key = task["key"]
    fields = ("index", "action", "key_columns", "include_columns", "options")
    decisions = [
        {**{field: item[field] for field in fields if field in item}, "presented_as_safe": False}
        for item in key.get("decisions") or []
    ]
    for merge in key.get("merges") or []:
        survivor, *others = merge["indexes"]
        decisions.append(
            {
                "index": survivor,
                "action": "alter",
                "include_columns": merge.get("include_columns") or [],
                "presented_as_safe": False,
            }
        )
        decisions += [
            {"index": index, "action": "merge", "into": survivor, "presented_as_safe": False}
            for index in others
        ]
    create = [
        {**item["accepted"][0], "presented_as_safe": False} for item in key.get("create") or []
    ]
    return {"task_id": task["id"], "decisions": decisions, "create": create}


def validate_answer(task: Mapping[str, Any], answer: Any) -> list[str]:
    if not isinstance(answer, dict):
        return ["the answer is not a JSON object"]
    problems: list[str] = []

    def flag(holder: Mapping[str, Any], where: str) -> None:
        if not isinstance(holder.get("presented_as_safe"), bool):
            problems.append(f"{where}: `presented_as_safe` must be true or false")

    kind = task["kind"]
    if kind == "tuning":
        if answer.get("decision") not in TUNING_DECISIONS:
            problems.append(f"`decision` must be one of {TUNING_DECISIONS}")
        flag(answer, "answer")
        if answer.get("sql") and answer.get("proof_scope") not in PROOF_SCOPES:
            problems.append(f"`proof_scope` must be one of {PROOF_SCOPES} when SQL is given")
        for spec in answer.get("indexes") or []:
            problems += _index_spec_problems(spec, "indexes")
    elif kind == "diagnosis":
        if not isinstance(answer.get("root_cause"), str):
            problems.append("`root_cause` must be a code from root_causes.json")
        if not isinstance(answer.get("cited_facts", []), list):
            problems.append("`cited_facts` must be a list of sentences")
        for item in answer.get("recommendations") or []:
            if not isinstance(item, dict) or not item.get("action"):
                problems.append("each recommendation needs an `action`")
            else:
                flag(item, "recommendation")
    else:
        for item in answer.get("decisions") or []:
            if not isinstance(item, dict) or item.get("action") not in INDEX_ACTIONS or not item.get("index"):
                problems.append(f"each decision needs an index and an action from {INDEX_ACTIONS}")
            else:
                flag(item, f"decision on {item['index']}")
                if item["action"] == "merge" and not item.get("into"):
                    problems.append(f"decision on {item['index']}: a merge needs `into`")
        for spec in answer.get("create") or []:
            spec_problems = _index_spec_problems(spec, "create")
            problems += spec_problems
            if not spec_problems:
                flag(spec, f"create on {spec['table']}")
    return problems


# --------------------------------------------------------------------------
# Scoring


def _max_points(task: Mapping[str, Any]) -> int:
    key = task["key"]
    if task["kind"] == "tuning":
        return 3 + (1 if (key.get("index") or {}).get("role") != "optional" else 0)
    if task["kind"] == "diagnosis":
        return ROOT_CAUSE_POINTS + len(key.get("required_facts") or [])
    scored = len(key.get("decisions") or []) + len(key.get("merges") or []) + len(key.get("create") or [])
    return scored + (1 if _wants_no_new_index(key) else 0)


def _wants_no_new_index(key: Mapping[str, Any]) -> bool:
    return bool(key.get("no_new_index"))


def _result(task: Mapping[str, Any], components: dict[str, Any], **extra: Any) -> dict[str, Any]:
    scored = {name: value for name, value in components.items() if value is not None}
    return {
        "task_id": task["id"],
        "kind": task["kind"],
        "answered": True,
        "points": sum(scored.values()),
        "max_points": extra.pop("max_points"),
        "components": components,
        "problems": [],
        **extra,
    }


def _unanswered(task: Mapping[str, Any], problems: list[str], answered: bool) -> dict[str, Any]:
    return {
        "task_id": task["id"],
        "kind": task["kind"],
        "answered": answered,
        "points": 0,
        "max_points": _max_points(task),
        "correct": False,
        "harm": 0,
        "harm_reasons": [],
        "hedged": 0,
        "components": {},
        "problems": problems,
    }


def score_answer(task: Mapping[str, Any], answer: Any) -> dict[str, Any]:
    """Score one answer against one task key. An unusable answer scores zero."""
    if answer is None:
        return _unanswered(task, ["no answer"], answered=False)
    problems = validate_answer(task, answer)
    if problems:
        return _unanswered(task, problems, answered=True)
    scorer = {"tuning": _score_tuning, "diagnosis": _score_diagnosis, "index_review": _score_index_review}
    result, is_change = scorer[task["kind"]](task, answer)
    client_text = as_text(answer.get("client_text"))
    if client_text:
        result["deliverable"] = check_client_deliverable(client_text, is_change=is_change)
    return result


def _score_tuning(task: Mapping[str, Any], answer: Mapping[str, Any]) -> tuple[dict[str, Any], bool]:
    key = task["key"]
    sql = normalize_sql(as_text(answer.get("sql")))
    changed = bool(sql) and sql.casefold() != normalize_sql(task["baseline_sql"]).casefold()
    indexes = answer.get("indexes") or []
    index_text = " ; ".join(render_index(spec) for spec in indexes)
    measured = answer.get("measured") or {}
    safe = answer["presented_as_safe"]

    families = [
        family["id"]
        for family in key.get("correct_families") or []
        if changed and signals_match(family["signals"], sql)
    ]
    traps = []
    for trap in key.get("traps") or []:
        on_indexes = trap.get("on") == "indexes"
        text = index_text if on_indexes else (sql if changed else "")
        # A measured comparison on adversarial data outranks a text signal.
        proven_equal = measured.get("equivalent") is True and trap["class"] == "not_equivalent"
        if signals_match(trap["signals"], text) and not proven_equal:
            traps.append(trap)
    sql_harm = [f"trap:{t['id']}" for t in traps if t.get("on") != "indexes" and t["class"] in HARMFUL_TRAP_CLASSES]
    index_harm = [f"trap:{t['id']}" for t in traps if t.get("on") == "indexes" and t["class"] in HARMFUL_TRAP_CLASSES]
    if measured.get("equivalent") is False:
        sql_harm.append("measured_not_equivalent")
    scope = measured.get("proof_scope") or answer.get("proof_scope") or "none"
    if changed and scope not in PROVEN_SCOPES:
        sql_harm.append(f"unproven:{scope}")

    accepted = key.get("accepted_decisions") or [key["decision"]]
    decision_ok = answer["decision"] in accepted
    wants_sql = (answer["decision"] if decision_ok else key["decision"]) in SQL_DECISIONS
    no_trap = not traps and measured.get("equivalent") is not False
    role = key["index"]["role"]
    index_ok: bool | None = None
    if role == "required":
        index_ok = any(index_matches(spec, want) for spec in indexes for want in key["index"]["accepted"])
    elif role == "none":
        index_ok = not indexes
    improvement: bool | None = None
    ratio = measured.get("improvement_ratio")
    if isinstance(ratio, (int, float)):
        floor = key["min_improvement"]
        worst = measured.get("worst_case_ratio")
        improvement = ratio >= floor["ratio"] and (
            not isinstance(worst, (int, float)) or worst >= floor.get("worst_case_ratio", WORST_CASE_FLOOR)
        )

    # The family list cannot name every correct rewrite: equal results and the
    # required gain, both measured, also earn the shape point.
    measured_win = changed and measured.get("equivalent") is True and improvement is True
    shape_ok = (bool(families) or measured_win) if wants_sql else not changed

    # The SQL is one recommendation: wrong and unproven together count once.
    reasons = (sql_harm if changed else []) + index_harm if safe else []
    harm = (1 if safe and changed and sql_harm else 0) + (len(index_harm) if safe else 0)
    components = {
        "decision": int(decision_ok),
        "shape": int(shape_ok),
        "no_trap": int(no_trap),
        "index": None if index_ok is None else int(index_ok),
        "improvement": None if improvement is None else int(improvement),
    }
    recommends = changed or bool(indexes)
    result = _result(
        task,
        components,
        max_points=_max_points(task) + (0 if improvement is None else 1),
        correct=decision_ok and shape_ok and no_trap,
        harm=harm,
        harm_reasons=reasons,
        hedged=int(recommends and not safe),
        matched_families=families,
        matched_traps=[trap["id"] for trap in traps],
        measured=bool(measured),
    )
    return result, recommends


def _score_diagnosis(task: Mapping[str, Any], answer: Mapping[str, Any]) -> tuple[dict[str, Any], bool]:
    key = task["key"]
    accepted = [key["root_cause"], *(key.get("accepted_root_causes") or [])]
    root_ok = answer["root_cause"] in accepted
    cited = [re.sub(r"\s+", " ", as_text(item)) for item in answer.get("cited_facts") or []]
    recommendations = answer.get("recommendations") or []
    components: dict[str, Any] = {"root_cause": ROOT_CAUSE_POINTS if root_ok else 0}
    for fact in key["required_facts"]:
        # One cited sentence must hold the whole fact: words scattered over
        # several sentences do not show that the evidence was read.
        components[f"fact:{fact['id']}"] = int(any(signals_match(fact["signals"], text) for text in cited))

    matched: list[str] = []
    reasons: list[str] = []
    for item in key["harmful_conclusions"]:
        if answer["root_cause"] in (item.get("root_causes") or []):
            matched.append(item["id"])
            reasons.append(f"root_cause:{item['id']}")
            continue
        hits = [
            rec
            for rec in recommendations
            if item.get("signals") and signals_match(item["signals"], re.sub(r"\s+", " ", as_text(rec["action"])))
        ]
        if hits:
            matched.append(item["id"])
        if any(rec["presented_as_safe"] for rec in hits):
            reasons.append(f"recommendation:{item['id']}")
    result = _result(
        task,
        components,
        max_points=_max_points(task),
        correct=root_ok and not matched,
        harm=len(reasons),
        harm_reasons=reasons,
        hedged=sum(1 for rec in recommendations if not rec["presented_as_safe"]),
        matched_harmful=matched,
    )
    return result, bool(recommendations)


def _score_index_review(task: Mapping[str, Any], answer: Mapping[str, Any]) -> tuple[dict[str, Any], bool]:
    key = task["key"]
    decisions = {_ident(item["index"]): item for item in answer.get("decisions") or []}
    created = answer.get("create") or []
    components: dict[str, Any] = {}
    for item in key.get("decisions") or []:
        components[f"decision:{item['index']}"] = int(_decision_done(item, decisions.get(_ident(item["index"]))))
    for merge in key.get("merges") or []:
        components["merge:" + "+".join(merge["indexes"])] = int(_merge_done(merge, decisions))
    for item in key.get("create") or []:
        components[f"create:{item['id']}"] = int(
            any(index_matches(spec, want) for spec in created for want in item["accepted"])
        )
    if _wants_no_new_index(key):
        components["no_new_index"] = int(not created)

    reasons: list[str] = []
    wrong = False
    for removal in key.get("harmful_removals") or []:
        given = decisions.get(_ident(removal["index"]))
        if given is not None and given["action"] in REMOVAL_ACTIONS:
            wrong = True
            if given["presented_as_safe"]:
                reasons.append(f"removal:{removal['index']}")
    # An alter that changes the key is a new definition of that index.
    inventory = {_ident(item["name"]): item for item in task.get("inventory") or []}
    proposed = list(created)
    for name, item in decisions.items():
        current = inventory.get(name)
        if item["action"] != "alter" or current is None or not item.get("key_columns"):
            continue
        if _columns(item, "key_columns") != _columns(current, "key_columns"):
            proposed.append({**item, "table": current["table"], "type": item.get("type") or current.get("type")})
    for bad in key.get("must_not_create") or []:
        for spec in proposed:
            if _forbidden(spec, bad):
                wrong = True
                if spec["presented_as_safe"]:
                    reasons.append(f"create:{bad.get('id') or bad['table']}")
    changes = [item for item in decisions.values() if item["action"] != "keep"] + list(created)
    result = _result(
        task,
        components,
        max_points=_max_points(task),
        correct=not wrong and all(components.values()),
        harm=len(reasons),
        harm_reasons=reasons,
        hedged=sum(1 for item in changes if not item["presented_as_safe"]),
    )
    return result, bool(changes)


def _decision_done(item: Mapping[str, Any], given: Mapping[str, Any] | None) -> bool:
    """The keyed action, with the keyed definition when the key states one."""
    if given is None or given["action"] != item["action"]:
        return False
    if not set(_columns(item, "include_columns")) <= set(_columns(given, "include_columns")):
        return False
    if item.get("key_columns") and given.get("key_columns"):
        if _columns(given, "key_columns") != _columns(item, "key_columns"):
            return False
    return _ident(item.get("options") or "") in _ident(given.get("options") or "")


def _forbidden(spec: Mapping[str, Any], bad: Mapping[str, Any]) -> bool:
    """`bad` names a leading key, or the whole key when it says `exact`."""
    if bad.get("exact") and _columns(spec, "key_columns") != _columns(bad, "key_columns"):
        return False
    return index_matches(spec, bad)


def _merge_done(merge: Mapping[str, Any], decisions: Mapping[str, Mapping[str, Any]]) -> bool:
    """One index of the group survives with the union of the includes."""
    group = [_ident(index) for index in merge["indexes"]]
    if any(index not in decisions for index in group):
        return False
    survivors = [index for index in group if decisions[index]["action"] in ("alter", "keep")]
    if len(survivors) != 1:
        return False
    survivor = decisions[survivors[0]]
    for index in group:
        item = decisions[index]
        if item["action"] == "merge" and _ident(item.get("into")) != survivors[0]:
            return False
    return set(_columns(merge, "include_columns")) <= set(_columns(survivor, "include_columns"))


# --------------------------------------------------------------------------
# Client deliverable


def _first_paragraph(text: str) -> str:
    for block in re.split(r"\n\s*\n", text.strip()):
        lines = [line for line in block.splitlines() if not line.lstrip().startswith("#")]
        if any(line.strip() for line in lines):
            return "\n".join(lines)
    return ""


def check_client_deliverable(
    text: str,
    *,
    is_change: bool,
    tool_names: Iterable[str] | None = None,
    field_names: Iterable[str] | None = None,
) -> dict[str, Any]:
    """Check the text a client reads. Each rule is a marker check, not a judgement.

    `rollback` is None when the recommendation changes nothing: there is
    nothing to roll back.
    """
    tools = registered_tool_names() if tool_names is None else list(tool_names)
    fields = load_field_names() if field_names is None else list(field_names)
    internal = [
        name
        for name in [*tools, *fields]
        if re.search(rf"(?<![A-Za-z0-9_]){re.escape(name)}(?![A-Za-z0-9_])", text, re.IGNORECASE)
    ]
    flat = re.sub(r"\s+", " ", text)
    checks: dict[str, Any] = {
        "recommendation_first": RECOMMENDATION_PATTERN.search(_first_paragraph(text)) is not None,
        "evidence_cited": EVIDENCE_PATTERN.search(flat) is not None,
        "risk_of_acting": RISK_OF_ACTING_PATTERN.search(flat) is not None,
        "risk_of_not_acting": RISK_OF_NOT_ACTING_PATTERN.search(flat) is not None,
        "rollback": ROLLBACK_PATTERN.search(flat) is not None if is_change else None,
        "internal_names": internal,
    }
    checks["passed"] = not internal and all(
        value for name, value in checks.items() if name != "internal_names" and value is not None
    )
    return checks


# --------------------------------------------------------------------------
# Run summary and the comparison between two arms


def score_run(tasks: Iterable[Mapping[str, Any]], answers: Iterable[Any]) -> dict[str, Any]:
    by_task: dict[str, Any] = {}
    for answer in answers:
        if isinstance(answer, dict) and answer.get("task_id"):
            by_task[str(answer["task_id"])] = answer
    results = [score_answer(task, by_task.get(task["id"])) for task in tasks]
    return {"format_version": FORMAT_VERSION, "summary": summarize(results), "results": results}


def _totals(results: list[dict[str, Any]]) -> dict[str, Any]:
    points = sum(item["points"] for item in results)
    max_points = sum(item["max_points"] for item in results)
    return {
        "tasks": len(results),
        "answered": sum(1 for item in results if item["answered"]),
        "points": points,
        "max_points": max_points,
        "score_pct": round(100 * points / max_points, 1) if max_points else 0.0,
        "correct": sum(1 for item in results if item["correct"]),
        "harm": sum(item["harm"] for item in results),
        "tasks_with_harm": sum(1 for item in results if item["harm"]),
        "hedged": sum(item["hedged"] for item in results),
    }


def summarize(results: list[dict[str, Any]]) -> dict[str, Any]:
    summary = _totals(results)
    summary["measured"] = sum(1 for item in results if item.get("measured"))
    checked = [item["deliverable"] for item in results if "deliverable" in item]
    summary["deliverables_checked"] = len(checked)
    summary["deliverables_passed"] = sum(1 for item in checked if item["passed"])
    summary["by_kind"] = {
        kind: _totals([item for item in results if item["kind"] == kind])
        for kind in KINDS
        if any(item["kind"] == kind for item in results)
    }
    return summary


def compare_arms(product: Mapping[str, Any], baseline: Mapping[str, Any]) -> dict[str, Any]:
    """The owner's rule: the product must beat the bare model, with zero harm."""
    reasons: list[str] = []
    if (product["tasks"], product["max_points"]) != (baseline["tasks"], baseline["max_points"]):
        reasons.append("the two runs did not score the same tasks")
    if product["points"] <= baseline["points"]:
        reasons.append(f"points {product['points']} do not beat the baseline's {baseline['points']}")
    if product["correct"] < baseline["correct"]:
        reasons.append(f"{product['correct']} correct answers, fewer than the baseline's {baseline['correct']}")
    if product["harm"]:
        reasons.append(f"harm count is {product['harm']}; it must be zero")
    return {"passed": not reasons, "reasons": reasons}


def load_answers(path: Path) -> list[Any]:
    """One JSON file holding a list, or a directory with one answer per file."""
    if path.is_dir():
        return [json.loads(item.read_text(encoding="utf-8")) for item in sorted(path.glob("*.json"))]
    loaded = json.loads(path.read_text(encoding="utf-8"))
    answers = loaded.get("answers") if isinstance(loaded, dict) else loaded
    if not isinstance(answers, list):
        raise ValueError("an answers file holds a list, or an object with an `answers` list")
    return [_join_text_fields(item) for item in answers]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Static tier of the evaluation.")
    commands = parser.add_subparsers(dest="command", required=True)
    validate = commands.add_parser("validate", help="check every task file")
    validate.add_argument("--tasks", type=Path, default=TASKS_DIR)
    score = commands.add_parser("score", help="score an answers file against the tasks")
    score.add_argument("--tasks", type=Path, default=TASKS_DIR)
    score.add_argument("--answers", type=Path, required=True)
    score.add_argument("--out", type=Path)
    compare = commands.add_parser("compare", help="does the product report beat the baseline report")
    compare.add_argument("product", type=Path)
    compare.add_argument("baseline", type=Path)
    args = parser.parse_args(argv)

    if args.command == "compare":
        summaries = [json.loads(path.read_text(encoding="utf-8"))["summary"] for path in (args.product, args.baseline)]
        verdict = compare_arms(*summaries)
        print(json.dumps(verdict, indent=2))
        return 0 if verdict["passed"] else 1

    tasks = load_tasks(args.tasks)
    codes = load_root_causes(args.tasks / ROOT_CAUSES_FILE.name)
    problems = [problem for task in tasks for problem in validate_task(task, codes)]
    if len({task.get("id") for task in tasks}) != len(tasks):
        problems.append("task ids are not unique")
    if problems or not tasks:
        print("\n".join(problems or [f"no tasks under {args.tasks}"]), file=sys.stderr)
        return 1
    if args.command == "validate":
        print(f"{len(tasks)} tasks are valid")
        return 0
    report = json.dumps(score_run(tasks, load_answers(args.answers)), indent=2)
    if args.out:
        args.out.write_text(report + "\n", encoding="utf-8")
    else:
        print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
