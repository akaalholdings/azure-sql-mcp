"""Fail-closed loading and lookup for the local Azure SQL database policy.

The policy is deliberately independent of server/configuration loading.  The
integration layer resolves ``AZURE_SQL_DATABASE_POLICY_FILE`` and passes that
path to :func:`load_database_policy`; this module never reads environment
variables or database state.

Version 1 documents a small, database-scoped allowlist::

    {
      "version": 1,
      "databases": {
        "appdb": {
          "environment": "development",
          "allow_read": true,
          "allow_benchmark": false,
          "allow_test_indexes": false,
          "test_index_name_prefixes": ["IX_Testing_"],
          "test_index_maxdop": 0,
          "allow_offline_test_index": false,
          "allow_view_apply": false,
          "allow_plan_apply": false,
          "allow_index_history_write": false,
          "application_intent": "read_write",
          "business_cycle_extension_days": 0,
          "max_benchmark_executions": 0,
          "max_tuning_candidates": 0,
          "max_tuning_session_executions": 0,
          "max_tuning_session_minutes": 0
        }
      }
    }

``test_index_name_prefixes`` lists the name prefixes a test index may take;
the workflow prefix ``IX_Testing_`` is the default and is always valid. The
tool generates the rest of the name. ``test_index_maxdop`` caps the build
(0 leaves the database default). ``allow_offline_test_index`` permits an
offline build, which blocks every writer of the table while it runs.

``application_intent`` is optional. ``read_only`` connects with
ApplicationIntent=ReadOnly, so a read scale-out replica is the diagnosis
target; the server then refuses to send a write to that database.

Missing database entries are denied by default.  The dangerous capabilities
also default to denied in the typed model, so an integration mistake cannot
turn a missing or partial policy into an apply permission.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path
from types import MappingProxyType
from typing import Any
from typing import Mapping


POLICY_VERSION = 1
_TOP_LEVEL_FIELDS = frozenset({"version", "databases"})
_DATABASE_FIELDS = frozenset(
    {
        "environment",
        "allow_read",
        "allow_benchmark",
        "allow_test_indexes",
        "test_index_name_prefixes",
        "test_index_maxdop",
        "allow_offline_test_index",
        "allow_view_apply",
        "allow_plan_apply",
        "allow_index_history_write",
        "application_intent",
        "business_cycle_extension_days",
        "history_sources",
        "max_benchmark_executions",
        "max_tuning_candidates",
        "max_tuning_session_executions",
        "max_tuning_session_minutes",
    }
)
APPLICATION_INTENTS = ("read_write", "read_only")
# The prefix of every test index the workflow names by default.
WORKFLOW_TEST_INDEX_PREFIX = "IX_Testing_"
MAX_TEST_INDEX_NAME_PREFIXES = 8
MAX_TEST_INDEX_MAXDOP = 64
# Long enough that a generated name cannot be taken for an application index.
_TEST_INDEX_NAME_PREFIX = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{3,31}$")
_REQUIRED_DATABASE_FIELDS = frozenset({"environment", "allow_read"})
_UNKNOWN_ENVIRONMENTS = frozenset({"", "none", "null", "unknown"})

# The environment label of a policy entry is the only source of truth for
# "is this production". A label counts as non-production only when it names a
# known non-production environment and no production word.
_PRODUCTION_WORDS = frozenset({"production", "prod", "prd", "live"})
_NON_PRODUCTION_WORDS = frozenset(
    {
        "development", "dev", "test", "testing", "qa", "uat", "sandbox", "staging",
        "stage", "preprod", "preproduction", "nonprod", "nonproduction", "integration",
        "ci", "local", "lab", "bench", "benchmark", "demo",
    }
)
_PRE_OR_NON_PRODUCTION = re.compile(r"\b(pre|non)[-_ ]?(prod|production)\b")
ENVIRONMENT_CLASSES = ("production", "non_production", "unrecognized")


def classify_environment(environment: str) -> str:
    """Classify a policy environment label.

    Returns ``production``, ``non_production`` or ``unrecognized``. The label
    is split into words ("prod-eu", "test_2"). Any production word makes it
    production, so "test-prod-copy" is production. A label with no known word
    is unrecognized; the gates treat it as production.
    """

    label = _PRE_OR_NON_PRODUCTION.sub(r"\1\2", environment.strip().casefold())
    words = {word for word in re.split(r"[^a-z0-9]+", label) if word}
    # A trailing number names an instance: "test2", "prod01".
    words |= {word.rstrip("0123456789") for word in words}
    if words & _PRODUCTION_WORDS:
        return "production"
    if words & _NON_PRODUCTION_WORDS:
        return "non_production"
    return "unrecognized"


class DatabasePolicyError(ValueError):
    """Base error for invalid or unusable policy documents."""


class DatabasePolicyLoadError(DatabasePolicyError):
    """Raised when a policy file cannot be read or decoded as JSON."""


class DatabasePolicyValidationError(DatabasePolicyError):
    """Raised when a decoded policy does not satisfy schema version 1."""


def _parse_history_sources(database_name: str, value: Any) -> Mapping[str, Any]:
    """Validate the optional ``history_sources`` object, one binding per recorder.

    Strict: an unknown recorder or a bad field stops the policy from loading. A
    recorder silently turned off would let "no events" read as a clean history.
    """

    if value is None:
        return MappingProxyType({})
    where = f"database {database_name!r} history_sources"
    if not isinstance(value, Mapping):
        raise DatabasePolicyValidationError(f"{where} must be an object.")
    # Imported here: the adapters import the server configuration.
    from .history_azure_monitor import AzureMetricsBinding
    from .history_azure_monitor import LogAnalyticsBinding
    from .history_database_watcher import DatabaseWatcherBinding
    from .history_loki import LokiBinding

    parsers = {
        "log_analytics": LogAnalyticsBinding.from_mapping,
        "azure_monitor_metrics": AzureMetricsBinding.from_mapping,
        "database_watcher": DatabaseWatcherBinding.from_mapping,
        "loki": LokiBinding.from_mapping,
    }
    unknown = sorted(str(key) for key in value if key not in parsers)
    if unknown:
        raise DatabasePolicyValidationError(
            f"{where} has unknown recorder(s): {', '.join(unknown)}. "
            f"Known: {', '.join(sorted(parsers))}."
        )
    bindings: dict[str, Any] = {}
    for name, binding in value.items():
        try:
            bindings[name] = parsers[name](database_name, binding)
        except ValueError as exc:
            raise DatabasePolicyValidationError(f"{where}.{name}: {exc}") from exc
    return MappingProxyType(bindings)


@dataclass(frozen=True)
class DatabasePolicy:
    """Capabilities granted to one named database."""

    database_name: str
    environment: str
    allow_read: bool
    allow_benchmark: bool = False
    allow_test_indexes: bool = False
    allow_view_apply: bool = False
    allow_plan_apply: bool = False
    allow_index_history_write: bool = False
    business_cycle_extension_days: int = 0
    max_benchmark_executions: int = 0
    max_tuning_candidates: int = 0
    max_tuning_session_executions: int = 0
    max_tuning_session_minutes: int = 0
    configured: bool = True
    application_intent: str = "read_write"
    test_index_name_prefixes: tuple[str, ...] = (WORKFLOW_TEST_INDEX_PREFIX,)
    test_index_maxdop: int = 0
    allow_offline_test_index: bool = False
    # Recorders this database's history can be read from: source name -> its
    # validated binding (history_sources.py). Empty when none is mapped.
    history_sources: Mapping[str, Any] = field(
        default_factory=lambda: MappingProxyType({}), compare=False, hash=False
    )

    def __post_init__(self) -> None:
        _validate_database_name(self.database_name, field_name="database_name")
        if not isinstance(self.configured, bool):
            raise DatabasePolicyValidationError("configured must be a boolean.")
        if (
            not isinstance(self.application_intent, str)
            or self.application_intent not in APPLICATION_INTENTS
        ):
            raise DatabasePolicyValidationError(
                f"application_intent must be one of: {', '.join(APPLICATION_INTENTS)}."
            )
        if self.configured:
            _validate_environment(self.environment)
        elif self.environment != "unknown":
            raise DatabasePolicyValidationError(
                "an unconfigured policy must use the 'unknown' environment sentinel."
            )
        for field_name in (
            "allow_read",
            "allow_benchmark",
            "allow_test_indexes",
            "allow_view_apply",
            "allow_plan_apply",
            "allow_index_history_write",
            "allow_offline_test_index",
        ):
            if not isinstance(getattr(self, field_name), bool):
                raise DatabasePolicyValidationError(f"{field_name} must be a boolean.")
        prefixes = self.test_index_name_prefixes
        if (
            not isinstance(prefixes, tuple)
            or not 1 <= len(prefixes) <= MAX_TEST_INDEX_NAME_PREFIXES
            or any(
                not isinstance(prefix, str) or not _TEST_INDEX_NAME_PREFIX.fullmatch(prefix)
                for prefix in prefixes
            )
            or len({prefix.casefold() for prefix in prefixes}) != len(prefixes)
        ):
            raise DatabasePolicyValidationError(
                "test_index_name_prefixes must be a list of 1 to "
                f"{MAX_TEST_INDEX_NAME_PREFIXES} different plain identifiers of 4 to 32 "
                "characters (letters, digits, underscores)."
            )
        if (
            not isinstance(self.test_index_maxdop, int)
            or isinstance(self.test_index_maxdop, bool)
            or not 0 <= self.test_index_maxdop <= MAX_TEST_INDEX_MAXDOP
        ):
            raise DatabasePolicyValidationError(
                f"test_index_maxdop must be an integer from 0 to {MAX_TEST_INDEX_MAXDOP}; "
                "0 leaves the database default."
            )
        for field_name in (
            "max_benchmark_executions",
            "max_tuning_candidates",
            "max_tuning_session_executions",
            "max_tuning_session_minutes",
            "business_cycle_extension_days",
        ):
            value = getattr(self, field_name)
            if (
                not isinstance(value, int)
                or isinstance(value, bool)
                or value < 0
            ):
                raise DatabasePolicyValidationError(
                    f"{field_name} must be a non-negative integer."
                )

    @classmethod
    def from_mapping(cls, database_name: str, value: Mapping[str, Any]) -> "DatabasePolicy":
        """Build one policy entry after strict field/type validation."""

        if not isinstance(value, Mapping):
            raise DatabasePolicyValidationError(
                f"policy for database {database_name!r} must be an object."
            )
        _validate_allowed_fields(value, _DATABASE_FIELDS, f"database {database_name!r}")
        _validate_required_fields(
            value,
            _REQUIRED_DATABASE_FIELDS,
            f"database {database_name!r}",
        )
        allow_benchmark = value.get("allow_benchmark", False)
        per_request_limit = value.get("max_benchmark_executions", 0)
        prefixes = value.get("test_index_name_prefixes", [WORKFLOW_TEST_INDEX_PREFIX])
        if not isinstance(prefixes, list):
            raise DatabasePolicyValidationError(
                f"database {database_name!r} test_index_name_prefixes must be a list."
            )
        return cls(
            database_name=database_name,
            environment=value["environment"],
            allow_read=value["allow_read"],
            allow_benchmark=allow_benchmark,
            allow_test_indexes=value.get("allow_test_indexes", False),
            allow_view_apply=value.get("allow_view_apply", False),
            allow_plan_apply=value.get("allow_plan_apply", False),
            allow_index_history_write=value.get("allow_index_history_write", False),
            max_benchmark_executions=per_request_limit,
            max_tuning_candidates=value.get(
                "max_tuning_candidates",
                10 if allow_benchmark and per_request_limit > 0 else 0,
            ),
            max_tuning_session_executions=value.get(
                "max_tuning_session_executions",
                per_request_limit,
            ),
            max_tuning_session_minutes=value.get(
                "max_tuning_session_minutes",
                20 if allow_benchmark and per_request_limit > 0 else 0,
            ),
            business_cycle_extension_days=value.get("business_cycle_extension_days", 0),
            application_intent=value.get("application_intent", "read_write"),
            test_index_name_prefixes=tuple(prefixes),
            test_index_maxdop=value.get("test_index_maxdop", 0),
            allow_offline_test_index=value.get("allow_offline_test_index", False),
            history_sources=_parse_history_sources(
                database_name, value.get("history_sources")
            ),
        )

    @classmethod
    def denied(cls, database_name: str) -> "DatabasePolicy":
        """Return a safe policy for an unconfigured database.

        ``environment`` is intentionally a sentinel rather than a usable
        environment.  It prevents an absent policy entry from being mistaken
        for a configured apply target by downstream gates.
        """

        return cls(
            database_name=database_name,
            environment="unknown",
            allow_read=False,
            allow_benchmark=False,
            allow_test_indexes=False,
            allow_view_apply=False,
            allow_plan_apply=False,
            allow_index_history_write=False,
            max_benchmark_executions=0,
            max_tuning_candidates=0,
            max_tuning_session_executions=0,
            max_tuning_session_minutes=0,
            business_cycle_extension_days=0,
            configured=False,
        )

    def can_benchmark(self, executions: int) -> bool:
        """Whether a bounded benchmark may run for this database."""

        return (
            self.allow_benchmark
            and isinstance(executions, int)
            and not isinstance(executions, bool)
            and 0 < executions <= self.max_benchmark_executions
        )

    def can_start_tuning_session(
        self,
        *,
        candidates: int,
        executions: int,
        minutes: int,
    ) -> bool:
        """Whether a complete durable tuning campaign fits local policy."""

        values = (candidates, executions, minutes)
        return (
            self.configured
            and self.allow_benchmark
            and all(
                isinstance(value, int)
                and not isinstance(value, bool)
                and value > 0
                for value in values
            )
            and candidates <= self.max_tuning_candidates
            and executions <= self.max_tuning_session_executions
            and minutes <= self.max_tuning_session_minutes
        )

    @property
    def environment_class(self) -> str:
        return classify_environment(self.environment)

    @property
    def is_non_production(self) -> bool:
        """True only for a label that names a known non-production environment."""

        return self.environment_class == "non_production"

    @property
    def is_production(self) -> bool:
        """True for a production label and for any label that is not known to
        be non-production: production safeguards apply unless proven otherwise."""

        return not self.is_non_production

    def can_apply_view(self) -> bool:
        """Whether reviewed view DDL may target this database."""

        return self.configured and self.is_non_production and self.allow_view_apply

    def can_write_index_history(self) -> bool:
        """Whether controlled append-only index telemetry may be recorded."""

        return self.configured and self.allow_read and self.allow_index_history_write

    def can_run_index_experiment(self) -> bool:
        """Whether temporary index DDL may target this database."""

        return self.configured and self.is_non_production and self.allow_test_indexes


@dataclass(frozen=True)
class DatabasePolicySet:
    """Validated versioned policy document with case-insensitive lookup."""

    version: int
    databases: Mapping[str, DatabasePolicy]
    source_path: Path | None = None
    _normalized: Mapping[str, DatabasePolicy] = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        if self.version != POLICY_VERSION:
            raise DatabasePolicyValidationError(
                f"unsupported database policy version: {self.version!r}."
            )
        normalized: dict[str, DatabasePolicy] = {}
        for name, policy in self.databases.items():
            _validate_database_name(name, field_name="database name")
            if not isinstance(policy, DatabasePolicy):
                raise DatabasePolicyValidationError(
                    f"policy for database {name!r} is not a DatabasePolicy."
                )
            if policy.database_name.casefold() != name.casefold():
                raise DatabasePolicyValidationError(
                    f"policy database_name does not match database key {name!r}."
                )
            folded = name.casefold()
            if folded in normalized:
                raise DatabasePolicyValidationError(
                    f"duplicate database names differing only by case: {name!r}."
                )
            normalized[folded] = policy
        object.__setattr__(self, "databases", MappingProxyType(dict(self.databases)))
        object.__setattr__(self, "_normalized", MappingProxyType(normalized))

    @classmethod
    def empty(cls, source_path: Path | None = None) -> "DatabasePolicySet":
        """Return a valid policy set that denies every database."""

        return cls(version=POLICY_VERSION, databases={}, source_path=source_path)

    @classmethod
    def from_mapping(
        cls,
        value: Mapping[str, Any],
        *,
        source_path: Path | None = None,
    ) -> "DatabasePolicySet":
        if not isinstance(value, Mapping):
            raise DatabasePolicyValidationError("database policy document must be an object.")
        _validate_exact_fields(value, _TOP_LEVEL_FIELDS, "top-level policy document")
        version = value["version"]
        if version != POLICY_VERSION or isinstance(version, bool):
            raise DatabasePolicyValidationError(
                f"policy version must be the integer {POLICY_VERSION}."
            )
        databases = value["databases"]
        if not isinstance(databases, Mapping):
            raise DatabasePolicyValidationError("policy databases must be an object.")
        parsed = {
            name: DatabasePolicy.from_mapping(name, entry)
            for name, entry in databases.items()
        }
        return cls(version=version, databases=parsed, source_path=source_path)

    def get(self, database_name: str) -> DatabasePolicy | None:
        """Return the configured policy, or ``None`` for an unknown database."""

        _validate_database_name(database_name, field_name="database_name")
        return self._normalized.get(database_name.casefold())

    def policy_for(self, database_name: str) -> DatabasePolicy:
        """Return a policy for lookup use, denying unknown databases."""

        return self.get(database_name) or DatabasePolicy.denied(database_name)

    def require(self, database_name: str) -> DatabasePolicy:
        """Return a configured policy or raise a fail-closed denial."""

        policy = self.get(database_name)
        if policy is None:
            raise PermissionError(
                f"database {database_name!r} has no configured database policy."
            )
        return policy

    def require_read(self, database_name: str) -> DatabasePolicy:
        """Return a policy that explicitly permits database reads."""

        policy = self.policy_for(database_name)
        if not policy.configured or not policy.allow_read:
            raise PermissionError("Database policy does not permit read access.")
        return policy

    def allows_read(self, database_name: str) -> bool:
        return self.policy_for(database_name).allow_read

    def application_intents(self) -> dict[str, str]:
        """The databases whose connections use a non-default application intent."""

        return {
            name: policy.application_intent
            for name, policy in self.databases.items()
            if policy.application_intent != "read_write"
        }

    def allows_benchmark(self, database_name: str, executions: int) -> bool:
        return self.policy_for(database_name).can_benchmark(executions)

    def allows_tuning_session(
        self,
        database_name: str,
        *,
        candidates: int,
        executions: int,
        minutes: int,
    ) -> bool:
        return self.policy_for(database_name).can_start_tuning_session(
            candidates=candidates,
            executions=executions,
            minutes=minutes,
        )

    def allows_test_indexes(self, database_name: str) -> bool:
        return self.policy_for(database_name).can_run_index_experiment()

    def allows_index_experiments(self, database_name: str) -> bool:
        return self.policy_for(database_name).can_run_index_experiment()

    def allows_view_apply(self, database_name: str) -> bool:
        return self.policy_for(database_name).can_apply_view()

    def allows_plan_apply(self, database_name: str) -> bool:
        return self.policy_for(database_name).allow_plan_apply

    def allows_index_history_write(self, database_name: str) -> bool:
        return self.policy_for(database_name).can_write_index_history()


def load_database_policy(path: str | Path) -> DatabasePolicySet:
    """Load and validate a local JSON policy file.

    Loading errors are explicit.  Callers that want an unavailable policy to
    behave as a safe deny-all configuration can use ``DatabasePolicySet.empty``
    after handling :class:`DatabasePolicyLoadError`.
    """

    if not isinstance(path, (str, Path)) or not str(path).strip():
        raise DatabasePolicyLoadError("a local database policy file path is required.")
    policy_path = Path(path).expanduser()
    try:
        raw = json.loads(policy_path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise DatabasePolicyLoadError(
            f"could not read database policy file {policy_path}: {exc}"
        ) from exc
    except json.JSONDecodeError as exc:
        raise DatabasePolicyLoadError(
            f"database policy file {policy_path} is not valid JSON: {exc.msg}"
        ) from exc
    try:
        return DatabasePolicySet.from_mapping(raw, source_path=policy_path)
    except DatabasePolicyError:
        raise
    except (KeyError, TypeError) as exc:
        raise DatabasePolicyValidationError(
            f"database policy file {policy_path} has an invalid schema: {exc}"
        ) from exc


def load_database_policy_or_deny(path: str | Path | None) -> DatabasePolicySet:
    """Load a policy, returning a deny-all set when no path is configured.

    This helper is intended for an integration boundary that treats an absent
    policy path as safe but wants malformed or unreadable files to remain
    visible and fail startup rather than silently granting access.
    """

    if path is None or (isinstance(path, str) and not path.strip()):
        return DatabasePolicySet.empty()
    return load_database_policy(path)


def _validate_exact_fields(
    value: Mapping[str, Any], allowed: frozenset[str], description: str
) -> None:
    _validate_allowed_fields(value, allowed, description)
    _validate_required_fields(value, allowed, description)


def _validate_allowed_fields(
    value: Mapping[str, Any], allowed: frozenset[str], description: str
) -> None:
    actual = set(value)
    unknown = sorted(actual - allowed)
    if unknown:
        raise DatabasePolicyValidationError(
            f"{description} has unknown field(s): {', '.join(unknown)}."
        )


def _validate_required_fields(
    value: Mapping[str, Any], required: frozenset[str], description: str
) -> None:
    missing = sorted(required - set(value))
    if missing:
        raise DatabasePolicyValidationError(
            f"{description} is missing required field(s): {', '.join(missing)}."
        )


def _validate_database_name(value: object, *, field_name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise DatabasePolicyValidationError(f"{field_name} must be a non-empty string.")


def _validate_environment(value: object) -> None:
    if (
        not isinstance(value, str)
        or not value.strip()
        or value.strip().casefold() in _UNKNOWN_ENVIRONMENTS
    ):
        raise DatabasePolicyValidationError(
            "environment must be a non-empty, known string."
        )


__all__ = [
    "ENVIRONMENT_CLASSES",
    "POLICY_VERSION",
    "WORKFLOW_TEST_INDEX_PREFIX",
    "DatabasePolicy",
    "DatabasePolicyError",
    "DatabasePolicyLoadError",
    "DatabasePolicySet",
    "DatabasePolicyValidationError",
    "classify_environment",
    "load_database_policy",
    "load_database_policy_or_deny",
]
