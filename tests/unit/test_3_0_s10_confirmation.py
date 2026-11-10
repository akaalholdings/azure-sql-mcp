"""Review finding 99: a production plan apply or rollback must not be
authorized by text the agent can make up.

The server issues a one-time token when it prepares the intent. Apply and
rollback on a production database need that token back. It is bound to one
intent, one fingerprint and one action, and it expires.
"""

from __future__ import annotations

import pytest

from azure_sql_mcp.admin_policy import ConfirmationError
from azure_sql_mcp.admin_policy import ConfirmationTokens
from azure_sql_mcp.database_policy import DatabasePolicy


def _policy(environment: str) -> DatabasePolicy:
    return DatabasePolicy.from_mapping(
        "appdb", {"environment": environment, "allow_read": True, "allow_plan_apply": True}
    )


class _Clock:
    def __init__(self) -> None:
        self.now = 1_000_000.0

    def __call__(self) -> float:
        return self.now


def _issue(tokens: ConfirmationTokens, **overrides) -> str:
    binding = {"intent_id": "intent-1", "fingerprint": "fp-1", "action": "apply", **overrides}
    return tokens.issue(**binding)["confirmation_token"]


def test_production_apply_is_refused_without_the_issued_token() -> None:
    tokens = ConfirmationTokens()
    _issue(tokens)

    for supplied in (None, "", "user approved", "CHG-12345"):
        with pytest.raises(ConfirmationError):
            tokens.verify_for_policy(
                _policy("production"),
                intent_id="intent-1",
                fingerprint="fp-1",
                action="apply",
                token=supplied,
            )


def test_the_issued_token_authorizes_exactly_one_apply() -> None:
    tokens = ConfirmationTokens()
    token = _issue(tokens)
    binding = {"intent_id": "intent-1", "fingerprint": "fp-1", "action": "apply"}

    tokens.verify_for_policy(_policy("production"), token=token, **binding)

    # A replay of the same call must not force the plan a second time.
    with pytest.raises(ConfirmationError):
        tokens.verify_for_policy(_policy("production"), token=token, **binding)


@pytest.mark.parametrize(
    "changed",
    [
        {"intent_id": "intent-2"},
        # The evidence or the prior state moved after the human read the intent.
        {"fingerprint": "fp-2"},
        # Approval to apply is not approval to roll back.
        {"action": "rollback"},
    ],
)
def test_token_is_bound_to_its_intent_fingerprint_and_action(changed: dict) -> None:
    tokens = ConfirmationTokens()
    token = _issue(tokens)
    binding = {"intent_id": "intent-1", "fingerprint": "fp-1", "action": "apply", **changed}

    with pytest.raises(ConfirmationError):
        tokens.verify_for_policy(_policy("production"), token=token, **binding)


def test_token_expires() -> None:
    clock = _Clock()
    tokens = ConfirmationTokens(ttl_seconds=600, clock=clock)
    issued = tokens.issue(intent_id="intent-1", fingerprint="fp-1", action="apply")
    assert issued["expires_in_seconds"] == 600

    clock.now += 601
    with pytest.raises(ConfirmationError, match="expired"):
        tokens.verify_for_policy(
            _policy("production"),
            intent_id="intent-1",
            fingerprint="fp-1",
            action="apply",
            token=issued["confirmation_token"],
        )


def test_wrong_guesses_cancel_the_token() -> None:
    tokens = ConfirmationTokens(max_attempts=3)
    token = _issue(tokens)
    binding = {"intent_id": "intent-1", "fingerprint": "fp-1", "action": "apply"}

    for _ in range(3):
        with pytest.raises(ConfirmationError):
            tokens.verify_for_policy(_policy("production"), token="AAAA-AAAA-AAAA", **binding)
    with pytest.raises(ConfirmationError):
        tokens.verify_for_policy(_policy("production"), token=token, **binding)


def test_a_new_token_replaces_the_old_one_for_the_same_intent() -> None:
    tokens = ConfirmationTokens()
    first = _issue(tokens)
    second = _issue(tokens)
    binding = {"intent_id": "intent-1", "fingerprint": "fp-1", "action": "apply"}

    assert first != second
    with pytest.raises(ConfirmationError):
        tokens.verify_for_policy(_policy("production"), token=first, **binding)


def test_token_is_not_kept_in_clear_text() -> None:
    tokens = ConfirmationTokens()
    token = _issue(tokens)

    assert token not in repr(vars(tokens))


@pytest.mark.parametrize("environment", ["production", "prd", "prod-eu", "customer-a"])
def test_production_and_unrecognized_environments_require_the_token(environment: str) -> None:
    tokens = ConfirmationTokens()

    assert tokens.required(_policy(environment)) is True
    with pytest.raises(ConfirmationError):
        tokens.verify_for_policy(
            _policy(environment),
            intent_id="intent-1",
            fingerprint="fp-1",
            action="apply",
            token="user approved",
        )


def test_non_production_apply_needs_no_token() -> None:
    tokens = ConfirmationTokens()

    assert tokens.required(_policy("test")) is False
    tokens.verify_for_policy(
        _policy("test"), intent_id="intent-1", fingerprint="fp-1", action="apply", token=None
    )


def test_unknown_action_is_refused_at_issue() -> None:
    with pytest.raises(ValueError):
        ConfirmationTokens().issue(intent_id="intent-1", fingerprint="fp-1", action="drop")
