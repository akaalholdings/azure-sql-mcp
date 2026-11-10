"""A database policy can forbid indexed-view designs (S14 item 5).

Some estates cannot use an indexed view at all: their queries are too complex
for one, or their loads use partition SWITCH, which fails while a view with an
index exists on the table. On such a database the product must not build one
and must not propose one. ``allow_indexed_view_designs: false`` says so.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest

from azure_sql_mcp import database_policy
from azure_sql_mcp.database_policy import DatabasePolicySet
from azure_sql_mcp.database_policy import DatabasePolicyValidationError
from azure_sql_mcp.tool_guides import get_tool_guide
from azure_sql_mcp.view_experiments import view_experiment_policy_blockers
from tests.unit.test_3_0_s13b2_indexed_view_tool import BODY
from tests.unit.test_3_0_s13b2_indexed_view_tool import _ViewSandbox
from tests.unit.test_3_0_s13b2_indexed_view_tool import _app
from tests.unit.test_3_0_s13b2_indexed_view_tool import _candidate_sql
from tests.unit.test_3_0_s13b2_indexed_view_tool import _policy
from tests.unit.test_3_0_s13b2_indexed_view_tool import _run
from tests.unit.test_3_0_s13b2_indexed_view_tool import _session

FIELD = "allow_indexed_view_designs"


def test_the_field_defaults_to_true_and_is_validated() -> None:
    assert _policy().policy_for("appdb").allow_indexed_view_designs is True
    assert _policy(**{FIELD: False}).policy_for("appdb").allow_indexed_view_designs is False
    with pytest.raises(DatabasePolicyValidationError, match=FIELD):
        _policy(**{FIELD: "no"})
    assert FIELD in str(database_policy.__doc__)


@pytest.mark.asyncio
@pytest.mark.parametrize("preview", [False, True])
async def test_a_forbidden_design_is_refused_before_any_statement_and_names_the_field(
    server_config_factory, monkeypatch: pytest.MonkeyPatch, preview: bool
) -> None:
    app = _app(server_config_factory, _policy(**{FIELD: False}))
    sandbox = _ViewSandbox(app, monkeypatch)
    session_id, candidate_id = _session(app, _candidate_sql())
    app.plans.profile_query = AsyncMock()  # type: ignore[method-assign]

    with pytest.raises(PermissionError, match=FIELD) as refusal:
        if preview:
            await app._benchmark_indexed_view_candidate("appdb", None, None, BODY, preview=True)
        else:
            await _run(app, session_id, candidate_id)

    assert "No query and no DDL was run" in str(refusal.value)
    assert sandbox.events == []
    app.plans.profile_query.assert_not_awaited()


def test_check_capabilities_reports_the_forbidden_design(server_config_factory) -> None:
    def block(policy: DatabasePolicySet | None = None) -> dict[str, Any]:
        app = _app(server_config_factory, policy)
        return app._local_view_experiment_capability(app.database_policy.policy_for("appdb"))

    forbidden = block(_policy(**{FIELD: False}))
    assert forbidden["allowed"] is False
    assert forbidden["designs_forbidden"] is True
    assert any(FIELD in blocker for blocker in forbidden["blockers"])

    allowed = block()
    assert allowed["allowed"] is True and allowed["designs_forbidden"] is False


def test_a_leased_view_can_still_be_dropped_after_the_field_is_set_to_false() -> None:
    """Cleanup reads the experiment permissions: the new field must not block a drop."""

    assert view_experiment_policy_blockers(_policy(**{FIELD: False}).policy_for("appdb")) == ()


def test_the_guide_and_the_tool_say_to_check_the_field_first(server_config_factory) -> None:
    guide = get_tool_guide("indexed_view_experiment")["guide"]
    assert FIELD in guide and "designs_forbidden" in guide
    assert guide.index("designs_forbidden") < guide.index("preview")

    app = _app(server_config_factory)
    tool = app.mcp._tool_manager.get_tool("benchmark_indexed_view_candidate")
    assert tool is not None and FIELD in str(tool.description)
