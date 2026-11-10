from __future__ import annotations

import json
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

import pytest

from azure_sql_mcp.plan_tree import is_true
from azure_sql_mcp.plans import SHOWPLAN_NAMESPACE
from azure_sql_mcp.plans import PlansService
from azure_sql_mcp.safe_sql import SafeSqlValidator

SHOWPLAN_FIXTURES = sorted(
    (Path(__file__).resolve().parents[1] / "fixtures" / "showplans").glob("*.xml")
)
NS = 'xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan"'


def _service() -> PlansService:
    return PlansService(executor=None, validator=SafeSqlValidator())  # type: ignore[arg-type]


# --- Oracle: the operator summary as it was before the one-pass walk. -------
# Kept verbatim so the equality test below proves the faster walk changes no
# byte of the output. Each helper scans the operator's whole subtree.


def _legacy_owned_descendants(
    service: PlansService,
    node: ET.Element,
    parent_map: dict[ET.Element, ET.Element],
    tag_name: str,
) -> list[ET.Element]:
    result: list[ET.Element] = []
    for child in node.iter():
        if child is node or service._local_name(child.tag) != tag_name:
            continue
        parent = parent_map.get(child)
        while parent is not None and parent is not node:
            if service._local_name(parent.tag) == "RelOp":
                break
            parent = parent_map.get(parent)
        if parent is node:
            result.append(child)
    return result


def _legacy_owned_diagnostic_elements(
    service: PlansService,
    node: ET.Element,
    parent_map: dict[ET.Element, ET.Element],
    tag_names: set[str],
) -> list[dict[str, Any]]:
    diagnostics: list[dict[str, Any]] = []
    for element in node.iter():
        if element is node or service._local_name(element.tag) not in tag_names:
            continue
        parent = parent_map.get(element)
        while parent is not None and parent is not node:
            if service._local_name(parent.tag) == "RelOp":
                break
            parent = parent_map.get(parent)
        if parent is not node:
            continue
        diagnostics.append(
            {
                "element": service._local_name(element.tag),
                **{key: value for key, value in element.attrib.items() if value != ""},
            }
        )
    return diagnostics


def _legacy_operator_predicates(
    service: PlansService,
    node: ET.Element,
    parent_map: dict[ET.Element, ET.Element],
) -> tuple[list[str], list[str]]:
    seek: list[str] = []
    residual: list[str] = []
    for element in _legacy_owned_descendants(service, node, parent_map, "ScalarOperator"):
        text = element.attrib.get("ScalarString")
        if not text:
            continue
        ancestors: list[str] = []
        parent = parent_map.get(element)
        while parent is not None and parent is not node:
            ancestors.append(service._local_name(parent.tag))
            parent = parent_map.get(parent)
        destination = seek if "SeekPredicates" in ancestors else residual
        if text not in destination:
            destination.append(text)
    return seek, residual


def _legacy_summarize_operator(
    service: PlansService,
    node: ET.Element,
    parent_map: dict[ET.Element, ET.Element],
) -> dict[str, Any]:
    estimated_rows = service._optional_float(node.attrib.get("EstimateRows"))
    counters = [
        service._summarize_thread_counter(counter)
        for counter in _legacy_owned_descendants(
            service, node, parent_map, "RunTimeCountersPerThread"
        )
    ]
    actual_rows = (
        sum(counter["actual_rows"] or 0.0 for counter in counters) if counters else None
    )
    estimated_without_row_goal = service._optional_float(
        node.attrib.get("EstimateRowsWithoutRowGoal")
    )
    row_goal = (
        is_true(node.attrib.get("IsRowGoal"))
        or estimated_without_row_goal is not None
        and estimated_rows is not None
        and estimated_without_row_goal > estimated_rows
    )
    top_nodes = _legacy_owned_descendants(service, node, parent_map, "Top")
    row_goal_details = {
        "detected": row_goal,
        "estimated_rows_without_row_goal": estimated_without_row_goal,
        "top_row_count": service._optional_float(top_nodes[0].attrib.get("RowCount"))
        if top_nodes
        else None,
        "source": "showplan_row_goal_attributes" if row_goal else None,
    }

    object_nodes = _legacy_owned_descendants(service, node, parent_map, "Object")
    object_node = object_nodes[0] if object_nodes else None
    object_payload = service._object_payload(object_node)
    index_scan_nodes = _legacy_owned_descendants(service, node, parent_map, "IndexScan")
    lookup = (
        node.attrib.get("PhysicalOp", "").lower() in {"key lookup", "rid lookup"}
        or any(is_true(scan.attrib.get("Lookup")) for scan in index_scan_nodes)
    )
    seek_predicates, residual_predicates = _legacy_operator_predicates(
        service, node, parent_map
    )
    spills = _legacy_owned_diagnostic_elements(
        service, node, parent_map, {"SpillToTempDb", "HashSpillDetails", "SortSpillDetails"}
    )
    implicit_conversions = _legacy_owned_diagnostic_elements(
        service, node, parent_map, {"PlanAffectingConvert", "ConvertIssue"}
    )
    operator_warnings = _legacy_owned_diagnostic_elements(
        service, node, parent_map, {"Warnings"}
    )
    execution_modes = sorted(
        {
            str(counter["execution_mode"])
            for counter in counters
            if counter.get("execution_mode")
        }
    )
    estimated_mode = node.attrib.get("EstimatedExecutionMode")
    parallel_exchange = service._parallel_exchange_payload(node, counters)

    return {
        "node_id": service._optional_int(node.attrib.get("NodeId")),
        "physical_op": node.attrib.get("PhysicalOp"),
        "logical_op": node.attrib.get("LogicalOp"),
        "object": object_payload,
        "object_name": object_payload.get("qualified_name") if object_payload else None,
        "index_name": object_payload.get("index") if object_payload else None,
        "seek_predicates": seek_predicates,
        "residual_predicates": residual_predicates,
        "estimated_rows": estimated_rows,
        "estimated_rows_without_row_goal": estimated_without_row_goal,
        "actual_rows": actual_rows,
        "actual_rows_ratio": (
            actual_rows / estimated_rows
            if actual_rows is not None and estimated_rows not in (None, 0)
            else None
        ),
        "estimate_error_ratio": (
            actual_rows / estimated_rows
            if actual_rows is not None and estimated_rows not in (None, 0)
            else None
        ),
        "estimated_io": service._optional_float(node.attrib.get("EstimateIO")),
        "estimated_cpu": service._optional_float(node.attrib.get("EstimateCPU")),
        "estimated_subtree_cost": service._optional_float(
            node.attrib.get("EstimatedTotalSubtreeCost")
        ),
        "estimated_execution_mode": estimated_mode,
        "execution_mode": execution_modes[0]
        if len(execution_modes) == 1
        else execution_modes or estimated_mode,
        "thread_counters": counters,
        "row_goal": row_goal,
        "row_goal_details": row_goal_details,
        "lookup": lookup,
        "parallel_exchange": parallel_exchange,
        "parallelism": parallel_exchange,
        "spills": spills,
        "implicit_conversions": implicit_conversions,
        "warnings": operator_warnings,
    }


def _legacy_operators(service: PlansService, raw_xml: str) -> list[dict[str, Any]]:
    root = ET.fromstring(raw_xml)
    parent_map = {child: parent for parent in root.iter() for child in parent}
    return [
        _legacy_summarize_operator(service, node, parent_map)
        for node in root.findall(".//sp:RelOp", SHOWPLAN_NAMESPACE)
    ]


def _current_operators(service: PlansService, raw_xml: str) -> list[dict[str, Any]]:
    operators = []
    for operator in service.summarize_showplan_xml(raw_xml)["operators"]:
        # Added by summarize_showplan_xml after the per-operator summary.
        operator = dict(operator)
        operator.pop("estimated_self_cost")
        operators.append(operator)
    return operators


def _dump(value: Any) -> str:
    # No sort_keys: key order is part of the payload the agent reads.
    return json.dumps(value)


# --- Generated plans ---------------------------------------------------------


def left_deep_plan(operator_count: int) -> str:
    """A left-deep nested-loops chain: the ORM and reporting shape that made the
    old walk quadratic. Every join's outer input is the next join."""

    joins = (operator_count - 1) // 2
    node_id = 2 * joins
    plan = _seek(node_id, table="T0")
    for level in range(1, joins + 1):
        join_id = node_id - 2 * level + 1
        plan = (
            f'<RelOp NodeId="{join_id - 1}" PhysicalOp="Nested Loops" LogicalOp="Inner Join" '
            f'EstimateRows="{level}" EstimatedTotalSubtreeCost="{0.01 * (level + 1):.4f}">'
            '<RunTimeInformation>'
            f'<RunTimeCountersPerThread Thread="0" ActualRows="{level * 3}" ActualExecutions="1" '
            'ActualElapsedms="2" ActualCPUms="1" ActualExecutionMode="Row"/>'
            "</RunTimeInformation>"
            "<Warnings><SpillToTempDb SpillLevel=\"1\"/></Warnings>"
            "<NestedLoops Optimized=\"0\">"
            f'<OuterReferences><ColumnReference Column="c{level}"/></OuterReferences>'
            f"{plan}{_seek(join_id, table=f'T{level}')}"
            "</NestedLoops></RelOp>"
        )
    return (
        f'<ShowPlanXML {NS}><BatchSequence><Batch><Statements>'
        '<StmtSimple StatementText="SELECT 1" StatementType="SELECT" StatementSubTreeCost="1">'
        f"<QueryPlan>{plan}</QueryPlan></StmtSimple>"
        "</Statements></Batch></BatchSequence></ShowPlanXML>"
    )


def _seek(node_id: int, *, table: str) -> str:
    return (
        f'<RelOp NodeId="{node_id}" PhysicalOp="Index Seek" LogicalOp="Index Seek" '
        'EstimateRows="1" EstimatedTotalSubtreeCost="0.0033">'
        '<RunTimeInformation>'
        '<RunTimeCountersPerThread Thread="0" ActualRows="1" ActualExecutions="3" '
        'ActualLogicalReads="6" ActualExecutionMode="Row"/>'
        "</RunTimeInformation>"
        '<IndexScan Ordered="1" Lookup="0">'
        f'<Object Database="[db]" Schema="[dbo]" Table="[{table}]" Index="[IX_{table}]"/>'
        "<SeekPredicates><SeekPredicateNew><SeekKeys><Prefix>"
        f'<RangeExpressions><ScalarOperator ScalarString="[{table}].[id]=[@p]"/></RangeExpressions>'
        "</Prefix></SeekKeys></SeekPredicateNew></SeekPredicates>"
        f'<Predicate><ScalarOperator ScalarString="[{table}].[flag]=(1)"/></Predicate>'
        "</IndexScan></RelOp>"
    )


# Ownership edge cases: a subquery operator nested inside a predicate, repeated
# and empty predicate text, warnings and conversions at two levels, two Top and
# Object elements under one operator, and parallel thread counters.
OWNERSHIP_EDGE_PLAN = f"""\
<ShowPlanXML {NS}><BatchSequence><Batch><Statements>
<StmtSimple StatementText="SELECT 1" StatementType="SELECT" StatementSubTreeCost="3">
<QueryPlan>
<Warnings><PlanAffectingConvert ConvertIssue="Seek Plan" Expression="plan-level"/></Warnings>
<RelOp NodeId="0" PhysicalOp="Top" LogicalOp="Top" EstimateRows="5" EstimateRowsWithoutRowGoal="50" EstimatedTotalSubtreeCost="3">
  <Top RowCount="0"><TopExpression><ScalarOperator ScalarString="(5)"/></TopExpression>
    <RelOp NodeId="1" PhysicalOp="Parallelism" LogicalOp="Gather Streams" Parallel="true" EstimateRows="50" EstimatedTotalSubtreeCost="2.5">
      <RunTimeInformation>
        <RunTimeCountersPerThread Thread="1" ActualRows="40" ActualExecutions="1" ActualExecutionMode="Batch"/>
        <RunTimeCountersPerThread Thread="2" ActualRows="10" ActualExecutions="1" ActualExecutionMode="Row"/>
      </RunTimeInformation>
      <Parallelism>
        <RelOp NodeId="2" PhysicalOp="Filter" LogicalOp="Filter" EstimateRows="50" EstimatedTotalSubtreeCost="2">
          <Warnings NoJoinPredicate="1" Empty="">
            <SpillToTempDb SpillLevel="2" SpilledThreadCount=""/>
            <SortSpillDetails GrantedMemoryKb="8"/>
            <PlanAffectingConvert ConvertIssue="Cardinality Estimate" Expression="CONVERT(int,[a])"/>
          </Warnings>
          <Filter StartupExpression="0">
            <RelOp NodeId="3" PhysicalOp="Key Lookup" LogicalOp="Key Lookup" EstimateRows="50" EstimatedTotalSubtreeCost="1">
              <Top RowCount="7"/><Top RowCount="9"/>
              <IndexScan Lookup="1">
                <Object Database="[db]" Schema="[dbo]" Table="[First]" Index="[PK_First]"/>
                <Object Database="[db]" Schema="[dbo]" Table="[Second]"/>
                <SeekPredicates><ScalarOperator ScalarString="[First].[id]=[x]"/><ScalarOperator ScalarString="[First].[id]=[x]"/></SeekPredicates>
                <Predicate><ScalarOperator ScalarString=""/><ScalarOperator ScalarString="[First].[id]=[x]"/></Predicate>
              </IndexScan>
              <Warnings><HashSpillDetails GrantedMemoryKb="16"/></Warnings>
            </RelOp>
            <Predicate>
              <ScalarOperator ScalarString="EXISTS(subquery)">
                <Subquery Operation="EXISTS">
                  <RelOp NodeId="4" PhysicalOp="Index Scan" LogicalOp="Index Scan" EstimateRows="1" EstimatedTotalSubtreeCost="0.5">
                    <Warnings><SpillToTempDb SpillLevel="9"/></Warnings>
                    <IndexScan><Object Table="[Inner]"/>
                      <Predicate><ScalarOperator ScalarString="[Inner].[k]=[outer].[k]"/></Predicate>
                    </IndexScan>
                  </RelOp>
                </Subquery>
              </ScalarOperator>
            </Predicate>
          </Filter>
        </RelOp>
      </Parallelism>
    </RelOp>
  </Top>
</RelOp>
</QueryPlan></StmtSimple>
</Statements></Batch></BatchSequence></ShowPlanXML>
"""


# --- Tests -------------------------------------------------------------------


def test_showplan_fixtures_exist() -> None:
    # An empty glob would make the parametrized equality test pass with no cases.
    assert len(SHOWPLAN_FIXTURES) >= 9


@pytest.mark.parametrize("fixture", SHOWPLAN_FIXTURES, ids=lambda path: path.stem)
def test_operator_summary_is_identical_to_the_legacy_walk_on_every_fixture(
    fixture: Path,
) -> None:
    service = _service()
    raw_xml = fixture.read_text(encoding="utf-8")

    assert _dump(_current_operators(service, raw_xml)) == _dump(
        _legacy_operators(service, raw_xml)
    )


@pytest.mark.parametrize(
    "raw_xml",
    [OWNERSHIP_EDGE_PLAN, left_deep_plan(41)],
    ids=["ownership_edges", "left_deep_41"],
)
def test_operator_summary_is_identical_to_the_legacy_walk_on_generated_plans(
    raw_xml: str,
) -> None:
    service = _service()

    assert _dump(_current_operators(service, raw_xml)) == _dump(
        _legacy_operators(service, raw_xml)
    )


def test_operator_owns_only_elements_above_its_child_operators() -> None:
    # The agent reads spills, conversions and predicates per node_id. If an
    # operator claimed its children's elements, the root would report every
    # spill in the plan and the slow operator could not be told apart.
    operators = {
        operator["node_id"]: operator
        for operator in _service().summarize_showplan_xml(OWNERSHIP_EDGE_PLAN)["operators"]
    }

    assert operators[0]["spills"] == []
    assert operators[0]["row_goal_details"]["top_row_count"] == 0.0
    assert operators[0]["residual_predicates"] == ["(5)"]
    assert [spill["element"] for spill in operators[2]["spills"]] == [
        "SpillToTempDb",
        "SortSpillDetails",
    ]
    assert operators[2]["spills"][0] == {"element": "SpillToTempDb", "SpillLevel": "2"}
    assert operators[2]["residual_predicates"] == ["EXISTS(subquery)"]
    assert operators[3]["object_name"] == "db.dbo.First"
    assert operators[3]["seek_predicates"] == ["[First].[id]=[x]"]
    assert operators[3]["residual_predicates"] == ["[First].[id]=[x]"]
    assert operators[3]["row_goal_details"]["top_row_count"] == 7.0
    assert operators[4]["spills"] == [{"element": "SpillToTempDb", "SpillLevel": "9"}]
    assert operators[4]["residual_predicates"] == ["[Inner].[k]=[outer].[k]"]
    assert operators[1]["execution_mode"] == ["Batch", "Row"]


def test_deep_left_deep_plan_summary_stays_fast() -> None:
    # summarize_showplan_xml runs synchronously inside async tools, so a slow
    # summary blocks every other tool call. The old walk rescanned each
    # operator's whole subtree about 8 times: quadratic on a left-deep chain.
    service = _service()
    raw_xml = left_deep_plan(801)
    assert raw_xml.count("<RelOp ") == 801

    best = float("inf")
    operator_count = 0
    for _ in range(3):
        started = time.perf_counter()
        operator_count = service.summarize_showplan_xml(raw_xml)["operator_count"]
        best = min(best, time.perf_counter() - started)

    assert operator_count == 801
    assert best < 0.25, f"801-operator left-deep plan took {best * 1000:.0f} ms"


# --- compact_plan_summary (response trimming for explain_query) --------------

_HEADLINE_KEYS = (
    "statement_count",
    "operator_count",
    "statements",
    "warnings",
    "spills",
    "implicit_conversions",
    "actual_metrics",
    "compile_metrics",
    "memory_grants",
    "missing_indexes",
    "parameters",
    "feedback",
    "feedback_state",
)


def test_compact_plan_summary_drops_operator_lists_and_keeps_the_rest() -> None:
    # The operator list grows about 1 KB per operator with no cap, and its
    # cost-ranked top_operators contradicts the digest's self-time ranking.
    from azure_sql_mcp.plans import compact_plan_summary

    summary = _service().summarize_showplan_xml(left_deep_plan(51))

    compact = compact_plan_summary(summary)

    assert "operators" not in compact
    assert "top_operators" not in compact
    assert "top_operators_by_estimated_cost" not in compact
    assert list(compact) == list(_HEADLINE_KEYS)
    for key in _HEADLINE_KEYS:
        assert compact[key] == summary[key]
    assert compact["operator_count"] == 51
    assert len(compact["spills"]) == 25
    assert len(_dump(compact)) * 5 < len(_dump(summary))


def test_compact_plan_summary_leaves_the_stored_summary_untouched() -> None:
    # In-process callers (benchmark profiles, plan comparison) read
    # artifact.summary["operators"] and ["top_operators"]; only the response
    # may be trimmed.
    from azure_sql_mcp.plans import compact_plan_summary

    summary = _service().summarize_showplan_xml(left_deep_plan(51))
    before = _dump(summary)

    compact_plan_summary(summary)
    compact_plan_summary(summary, include_operator_summary=True)

    assert _dump(summary) == before
    assert len(summary["operators"]) == 51
    assert len(summary["top_operators"]) == 5


def test_full_operator_summary_names_the_cost_ranked_list() -> None:
    # Cost is an estimate in every plan. A list named top_operators next to the
    # digest's self-time top_operators reads as a second "slowest operators".
    from azure_sql_mcp.plans import compact_plan_summary

    summary = _service().summarize_showplan_xml(left_deep_plan(51))

    full = compact_plan_summary(summary, include_operator_summary=True)

    assert "top_operators" not in full
    assert full["operators"] == summary["operators"]
    assert full["top_operators_by_estimated_cost"] == summary["top_operators"]
    assert {key: full[key] for key in _HEADLINE_KEYS} == {
        key: summary[key] for key in _HEADLINE_KEYS
    }
