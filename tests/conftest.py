from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest

from azure_sql_mcp.config import AccessMode
from azure_sql_mcp.config import AuthMode
from azure_sql_mcp.config import ServerConfig
from azure_sql_mcp.config import ToolGroup
from azure_sql_mcp.config import TransportConfig
from azure_sql_mcp.config import TransportMode
from azure_sql_mcp.config import WritePolicy


@pytest.fixture
def server_config_factory(tmp_path: Path) -> Callable[..., ServerConfig]:
    def factory(**overrides) -> ServerConfig:
        config = {
            "server": "server.database.windows.net",
            "default_database": "appdb",
            "allowed_databases": ("appdb", "reportingdb"),
            "auth_mode": AuthMode.ENTRA_DEFAULT,
            "access_mode": AccessMode.RESTRICTED,
            "query_timeout_seconds": 30,
            "row_limit": 200,
            "pool_size": 5,
            "max_retries": 3,
            "tool_timeout_seconds": 45,
            "log_format": "text",
            "username": None,
            "password": None,
            "trust_server_certificate": False,
            "tenant_id": None,
            "client_id": None,
            "client_secret": None,
            "transport": TransportConfig(
                mode=TransportMode.STDIO,
                host="127.0.0.1",
                port=8000,
            ),
            "tool_groups": frozenset({ToolGroup.ALL}),
            "log_level": "INFO",
            "mcp_bearer_token": None,
            "write_policy": WritePolicy.DISABLED,
            "audit_dir": str(tmp_path / "audit"),
            "audit_full_sql": False,
            "remote_admin_enabled": False,
            "performance_state_dir": ":memory:",
        }
        config.update(overrides)
        return ServerConfig(**config)

    return factory


@pytest.fixture
def sample_server_config(server_config_factory: Callable[..., ServerConfig]) -> ServerConfig:
    return server_config_factory()


SHOWPLAN_NS = "http://schemas.microsoft.com/sqlserver/2004/07/showplan"


class ShowplanBuilder:
    """Synthetic showplan XML: real element names, only the attributes a test needs."""

    def plan(
        self,
        root_relop: str,
        *,
        cost: float = 1.0,
        statement_attrs: str = "",
        plan_attrs: str = "",
        plan_children: str = "",
        text: str = "SELECT 1",
    ) -> str:
        return (
            f'<ShowPlanXML xmlns="{SHOWPLAN_NS}" Version="1.564"><BatchSequence><Batch><Statements>'
            f'<StmtSimple StatementText="{text}" StatementType="SELECT" StatementSubTreeCost="{cost}" {statement_attrs}>'
            f"<QueryPlan {plan_attrs}>{plan_children}{root_relop}</QueryPlan>"
            "</StmtSimple></Statements></Batch></BatchSequence></ShowPlanXML>"
        )

    def runtime(self, *threads: tuple, mode: str = "Row") -> str:
        """Each thread is (thread, rows, executions, elapsed_ms, cpu_ms[, rows_read]); None omits a time."""

        counters = []
        for thread in threads:
            number, rows, executions, elapsed, cpu = thread[:5]
            extra = f' ActualElapsedms="{elapsed}"' if elapsed is not None else ""
            extra += f' ActualCPUms="{cpu}"' if cpu is not None else ""
            extra += f' ActualRowsRead="{thread[5]}"' if len(thread) > 5 else ""
            counters.append(
                f'<RunTimeCountersPerThread Thread="{number}" ActualRows="{rows}" '
                f'ActualExecutions="{executions}" ActualExecutionMode="{mode}"{extra} />'
            )
        return f"<RunTimeInformation>{''.join(counters)}</RunTimeInformation>"

    def relop(
        self,
        node_id: int,
        physical: str,
        logical: str | None = None,
        *children: str,
        wrapper: str | None = None,
        est_rows: float = 1.0,
        cost: float = 1.0,
        runtime: str = "",
        attrs: str = "",
        body: str = "",
        outputs: str = "",
        warnings: str = "",
    ) -> str:
        tag = wrapper or physical.replace(" ", "")
        return (
            f'<RelOp NodeId="{node_id}" PhysicalOp="{physical}" LogicalOp="{logical or physical}" '
            f'EstimateRows="{est_rows}" EstimatedTotalSubtreeCost="{cost}" {attrs}>'
            f"<OutputList>{outputs}</OutputList>{warnings}{runtime}"
            f"<{tag}>{body}{''.join(children)}</{tag}></RelOp>"
        )

    def scan(
        self,
        node_id: int,
        table: str = "T",
        *,
        physical: str = "Clustered Index Scan",
        index: str = "PK_T",
        est_rows: float = 1.0,
        cost: float = 1.0,
        runtime: str = "",
        attrs: str = "",
        body: str = "",
        outputs: str = "",
    ) -> str:
        obj = f'<Object Database="[db]" Schema="[dbo]" Table="[{table}]" Index="[{index}]" IndexKind="Clustered" />'
        return self.relop(
            node_id,
            physical,
            physical,
            wrapper="IndexScan",
            est_rows=est_rows,
            cost=cost,
            runtime=runtime,
            attrs=attrs,
            body=obj + body,
            outputs=outputs,
        )

    def column(self, table: str, column: str) -> str:
        return f'<ColumnReference Database="[db]" Schema="[dbo]" Table="[{table}]" Column="{column}" />'

    def seek_keys(self, table: str, column: str, value: str, *, scan_type: str = "EQ") -> str:
        """A SeekPredicateNew comparing ``table.column`` to a constant or ``@variable``."""

        operand = (
            f'<Identifier><ColumnReference Column="{value}" /></Identifier>'
            if value.startswith("@")
            else f'<Const ConstValue="{value}" />'
        )
        return (
            f'<SeekPredicateNew><SeekKeys><Prefix ScanType="{scan_type}"><RangeColumns>{self.column(table, column)}'
            f'</RangeColumns><RangeExpressions><ScalarOperator ScalarString="{value}">{operand}</ScalarOperator>'
            "</RangeExpressions></Prefix></SeekKeys></SeekPredicateNew>"
        )


@pytest.fixture
def showplan() -> ShowplanBuilder:
    return ShowplanBuilder()
