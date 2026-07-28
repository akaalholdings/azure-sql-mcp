"""Evidence-governed learning lifecycle, retrieval, and handoff rules."""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import timedelta
from typing import Any, Callable, Iterable, Mapping, Sequence

from .learning_contracts import (
    DEFAULT_FRESHNESS_DAYS,
    DecisionRecordV1,
    HandoffV1,
    LessonV1,
    OutcomeReviewV1,
    parse_timestamp,
    utc_now,
)
from .learning_store import ContractNotFoundError, LearningStore, LifecycleError


class LearningServiceError(ValueError):
    """Raised when a learning request violates evidence or lifecycle policy."""


Clock = Callable[[], str]
EvidenceValidator = Callable[[str, DecisionRecordV1], bool]
_SCOPE_KEYS = (
    "skill_version",
    "server_fingerprint",
    "database_fingerprint",
    "runtime_compatibility_fingerprint",
    "tool_schema_fingerprint",
    "sanitized_config_fingerprint",
)


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _dedupe(values: Iterable[str | None]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(value for value in values if value))


def _dedupe_mappings(values: Iterable[Mapping[str, Any]]) -> tuple[Mapping[str, Any], ...]:
    result: list[Mapping[str, Any]] = []
    seen: set[str] = set()
    for value in values:
        item = dict(value)
        key = _canonical(item)
        if key not in seen:
            seen.add(key)
            result.append(item)
    return tuple(result)


def _latest_timestamp(*values: str) -> str:
    return max(values, key=lambda item: parse_timestamp(item, "timestamp"))


class LearningService:
    """Deterministic learning plane; only the maintainer CLI can activate lessons."""

    def __init__(
        self,
        store: LearningStore,
        *,
        clock: Clock = utc_now,
        freshness_days: int = DEFAULT_FRESHNESS_DAYS,
        evidence_validator: EvidenceValidator | None = None,
    ) -> None:
        if freshness_days <= 0:
            raise ValueError("freshness_days must be greater than zero.")
        self.store = store
        self.clock = clock
        self.freshness_days = freshness_days
        self.evidence_validator = evidence_validator

    @staticmethod
    def _decision_scope(decision: DecisionRecordV1) -> dict[str, Any]:
        scope = dict(decision.scope)
        # The process runtime is provenance on a decision, not a learning
        # compatibility dimension.  Never let it partition recall or scope.
        scope.pop("runtime_fingerprint", None)
        scope.pop("compatibility_fingerprint", None)
        scope.setdefault("skill_version", decision.skill_version)
        scope.setdefault(
            "runtime_compatibility_fingerprint",
            decision.runtime_compatibility_fingerprint,
        )
        if decision.tool_schema_fingerprint is not None:
            scope.setdefault("tool_schema_fingerprint", decision.tool_schema_fingerprint)
        if decision.sanitized_config_fingerprint is not None:
            scope.setdefault("sanitized_config_fingerprint", decision.sanitized_config_fingerprint)
        return scope

    @classmethod
    def _scope_key(cls, decision: DecisionRecordV1) -> str:
        return _canonical(
            {
                key: value
                for key, value in cls._decision_scope(decision).items()
                if key in _SCOPE_KEYS
            }
        )

    def _validate_decision_lineage(self, decision: DecisionRecordV1) -> None:
        current_created = parse_timestamp(
            decision.created_at_utc,
            "decision.created_at_utc",
        )
        current_scope = self._scope_key(decision)
        for review_id in decision.based_on_review_ids:
            try:
                review = self.store.get_review(review_id)
                prior = self.store.get_decision(review.decision_id)
            except ContractNotFoundError as exc:
                raise LearningServiceError(
                    f"Decision lineage references unknown review {review_id}."
                ) from exc
            if prior.decision_id == decision.decision_id:
                raise LearningServiceError(
                    "A decision cannot be based on a review of itself."
                )
            if (
                prior.skill != decision.skill
                or prior.skill_version != decision.skill_version
                or prior.learning_key != decision.learning_key
                or self._scope_key(prior) != current_scope
            ):
                raise LearningServiceError(
                    "Decision lineage review is outside the compatible scope."
                )
            if prior.lifecycle != "reviewed" or not review.complete:
                raise LearningServiceError(
                    "Decision lineage must reference a reviewed prior decision."
                )
            prior_created = parse_timestamp(
                prior.created_at_utc,
                "prior decision.created_at_utc",
            )
            review_created = parse_timestamp(
                review.completed_at_utc or review.created_at_utc,
                "review timestamp",
            )
            if prior_created >= current_created or review_created >= current_created:
                raise LearningServiceError(
                    "Decision lineage must precede the new decision."
                )

    def record_decision(
        self,
        decision: DecisionRecordV1,
        *,
        idempotency_key: str | None = None,
        request_fingerprint: str | None = None,
    ) -> DecisionRecordV1:
        scope = self._decision_scope(decision)
        for key, expected in (
            (
                "runtime_compatibility_fingerprint",
                decision.runtime_compatibility_fingerprint,
            ),
            ("tool_schema_fingerprint", decision.tool_schema_fingerprint),
            ("sanitized_config_fingerprint", decision.sanitized_config_fingerprint),
        ):
            if expected is None:
                continue
            if scope.get(key) != expected:
                raise LearningServiceError(
                    f"Decision scope {key} does not match its recorded fingerprint."
                )
        self._validate_decision_lineage(decision)
        for lesson_id in decision.applied_lesson_ids:
            lesson = self.store.get_lesson(lesson_id)
            if lesson.status != "active" or decision.skill not in lesson.applicable_skills:
                raise LearningServiceError(
                    f"Applied lesson {lesson_id} is not active for {decision.skill}."
                )
            if not self._scope_compatible(lesson, scope):
                raise LearningServiceError(
                    f"Applied lesson {lesson_id} is outside the decision scope."
                )
        if self.evidence_validator is not None:
            invalid = [
                ref
                for ref in decision.consumed_evidence_refs
                if not self.evidence_validator(ref, decision)
            ]
            if invalid:
                raise LearningServiceError(
                    "Decision references evidence that is unavailable in its scope."
                )
        try:
            return self.store.create_decision(
                decision,
                idempotency_key=idempotency_key,
                request_fingerprint=request_fingerprint,
            )
        except LifecycleError as exc:
            raise LearningServiceError(str(exc)) from exc

    def record_outcome_review(
        self,
        review: OutcomeReviewV1,
        *,
        idempotency_key: str | None = None,
        request_fingerprint: str | None = None,
    ) -> OutcomeReviewV1:
        decision = self.store.get_decision(review.decision_id)
        if parse_timestamp(
            review.created_at_utc, "review.created_at_utc"
        ) < parse_timestamp(decision.created_at_utc, "decision.created_at_utc"):
            raise LearningServiceError("Outcome review chronology precedes its decision.")
        for ref in review.terminal_evidence_refs:
            if not self.store.terminal_link_exists(ref, decision_id=decision.decision_id):
                raise LearningServiceError(
                    "Outcome review references an unknown or cross-scope terminal link."
                )
        stored = self.store.create_review(
            review,
            idempotency_key=idempotency_key,
            request_fingerprint=request_fingerprint,
        )
        if decision.lifecycle == "recorded":
            reviewed_at = _latest_timestamp(
                decision.updated_at_utc,
                stored.created_at_utc,
            )
            self.store.save_decision(
                replace(
                    decision,
                    lifecycle="reviewed",
                    updated_at_utc=reviewed_at,
                    version=decision.version + 1,
                ),
                expected_version=decision.version,
                idempotency_key=f"decision-reviewed:{stored.review_id}",
            )
        self._refresh_proposals(decision)
        self._quarantine_applied_lessons(decision)
        return stored

    def _related_reviews(
        self,
        decision: DecisionRecordV1,
    ) -> list[tuple[DecisionRecordV1, OutcomeReviewV1]]:
        related: list[tuple[DecisionRecordV1, OutcomeReviewV1]] = []
        for review in self.store.list_reviews():
            candidate = self.store.get_decision(review.decision_id)
            if (
                candidate.skill == decision.skill
                and candidate.skill_version == decision.skill_version
                and candidate.learning_key == decision.learning_key
                and self._scope_key(candidate) == self._scope_key(decision)
            ):
                related.append((candidate, review))
        return related

    @staticmethod
    def _urgent(review: OutcomeReviewV1) -> bool:
        return review.complete and (
            review.has_safety_or_equivalence_failure
            or review.has_cleanup_failure
            or review.explicit_correction
        )

    @staticmethod
    def _contradictory(review: OutcomeReviewV1) -> bool:
        return review.complete and (
            review.alignment == "contradiction"
            or review.has_safety_or_equivalence_failure
            or review.has_cleanup_failure
            or review.has_material_contradiction
            or review.explicit_correction
        )

    @classmethod
    def _eligible(
        cls,
        related: Sequence[tuple[DecisionRecordV1, OutcomeReviewV1]],
    ) -> bool:
        complete = [item for item in related if item[1].complete]
        aligned = [
            item
            for item in complete
            if item[1].is_aligned_complete
            and item[1].causal_strength in {"moderate", "strong"}
        ]
        return (
            len(aligned) >= 3
            and len(_dedupe(item[0].session_id for item in aligned)) >= 2
            and len(_dedupe(item[0].subject_fingerprint for item in aligned)) >= 2
            and not any(cls._contradictory(review) for _decision, review in complete)
        )

    def propose_lesson(
        self,
        *,
        learning_key: str,
        review_ids: Sequence[str],
        trigger: Mapping[str, Any],
        action: Mapping[str, Any],
        preconditions: Mapping[str, Any],
        counterexamples: Sequence[Mapping[str, Any]],
        next_observation: Mapping[str, Any] | None = None,
        required_evidence: Sequence[str],
        applicable_skills: Sequence[str],
        applicable_scopes: Sequence[Mapping[str, Any]] = (),
        tags: Sequence[str] = (),
        freshness_days: int | None = None,
        supersedes_lesson_id: str | None = None,
        idempotency_key: str | None = None,
    ) -> LessonV1:
        if not review_ids:
            raise LearningServiceError("A lesson proposal requires reviewed evidence.")
        if len(set(review_ids)) != len(review_ids):
            raise LearningServiceError("A lesson proposal cannot repeat a review identifier.")
        reviews = [self.store.get_review(review_id) for review_id in review_ids]
        if any(not review.complete for review in reviews):
            raise LearningServiceError("A lesson proposal requires complete terminal reviews.")
        decisions = [self.store.get_decision(review.decision_id) for review in reviews]
        anchor = decisions[0]
        if learning_key != anchor.learning_key:
            raise LearningServiceError("Lesson learning_key does not match its decisions.")
        for decision in decisions[1:]:
            if (
                decision.skill != anchor.skill
                or decision.skill_version != anchor.skill_version
                or decision.learning_key != anchor.learning_key
                or self._scope_key(decision) != self._scope_key(anchor)
            ):
                raise LearningServiceError(
                    "Lesson support reviews must share skill, version, key, and scope."
                )
        if anchor.skill not in set(applicable_skills):
            raise LearningServiceError(
                "A lesson must remain applicable to its source skill."
            )
        urgent = any(self._urgent(review) for review in reviews)
        selected = list(zip(decisions, reviews, strict=True))
        eligible = self._eligible(selected) and not urgent
        allowed_scopes = _dedupe_mappings(
            self._decision_scope(decision) for decision in decisions
        )
        requested_scopes = (
            _dedupe_mappings(applicable_scopes)
            if applicable_scopes
            else allowed_scopes
        )
        allowed_scope_keys = {_canonical(scope) for scope in allowed_scopes}
        if any(_canonical(scope) not in allowed_scope_keys for scope in requested_scopes):
            raise LearningServiceError(
                "Normal MCP clients cannot broaden a lesson beyond its evidence scope."
            )
        support_refs = _dedupe(review.review_id for review in reviews)
        contradiction_refs = _dedupe(
            review.review_id for review in reviews if self._contradictory(review)
        )
        support_sessions = _dedupe(decision.session_id for decision in decisions)
        support_queries = _dedupe(
            decision.query_fingerprint for decision in decisions
        )
        latest_support = max(
            (review.completed_at_utc or review.created_at_utc for review in reviews),
            key=lambda value: parse_timestamp(value, "review timestamp"),
        )
        now = _latest_timestamp(self.clock(), latest_support)
        lesson = LessonV1(
            learning_key=learning_key,
            subject_kind=anchor.subject_kind,
            subject_fingerprint=anchor.subject_fingerprint,
            trigger=dict(trigger),
            action=dict(action),
            preconditions=dict(preconditions),
            counterexamples=tuple(dict(item) for item in counterexamples),
            next_observation=dict(next_observation) if next_observation is not None else None,
            required_evidence=tuple(required_evidence),
            applicable_skills=tuple(applicable_skills),
            applicable_scopes=requested_scopes,
            query_fingerprints=support_queries,
            tags=tuple(tags),
            support_refs=support_refs,
            based_on_review_ids=support_refs,
            contradiction_refs=contradiction_refs,
            freshness_days=freshness_days or self.freshness_days,
            created_at_utc=now,
            updated_at_utc=now,
            status_changed_at_utc=now,
            last_supported_at_utc=latest_support,
            status="eligible" if eligible else "proposed",
            proposal_kind="urgent" if urgent else "normal",
            supersedes_lesson_id=supersedes_lesson_id,
            support_session_ids=support_sessions,
            support_query_fingerprints=support_queries,
        )
        return self.store.create_lesson(
            lesson,
            idempotency_key=idempotency_key,
        )

    def _refresh_proposals(self, decision: DecisionRecordV1) -> None:
        related = self._related_reviews(decision)
        related_review_ids = {review.review_id for _candidate, review in related}
        complete = [item for item in related if item[1].complete]
        if not complete:
            return
        support_refs = _dedupe(review.review_id for _candidate, review in complete)
        contradictions = _dedupe(
            review.review_id
            for _candidate, review in complete
            if self._contradictory(review)
        )
        sessions = _dedupe(candidate.session_id for candidate, _review in complete)
        queries = _dedupe(
            candidate.query_fingerprint for candidate, _review in complete
        )
        latest_support = max(
            (review.completed_at_utc or review.created_at_utc for _candidate, review in complete),
            key=lambda value: parse_timestamp(value, "review timestamp"),
        )
        for lesson in self.store.list_lessons():
            if (
                lesson.status not in {"proposed", "eligible", "active"}
                or (
                    lesson.status != "active"
                    and lesson.proposal_kind != "normal"
                )
                or lesson.learning_key != decision.learning_key
                or decision.skill not in lesson.applicable_skills
                or not set(lesson.support_refs).issubset(related_review_ids)
                or not self._scope_compatible(lesson, self._decision_scope(decision))
            ):
                continue
            status = (
                "active"
                if lesson.status == "active"
                else ("eligible" if self._eligible(related) else "proposed")
            )
            refreshed_support_refs = _dedupe((*lesson.support_refs, *support_refs))
            refreshed_based_on = _dedupe(
                (*lesson.based_on_review_ids, *support_refs)
            )
            refreshed_contradictions = _dedupe(
                (*lesson.contradiction_refs, *contradictions)
            )
            refreshed_sessions = _dedupe(
                (*lesson.support_session_ids, *sessions)
            )
            refreshed_queries = _dedupe(
                (*lesson.support_query_fingerprints, *queries)
            )
            refreshed_query_fingerprints = _dedupe(
                (*lesson.query_fingerprints, *queries)
            )
            if (
                status == lesson.status
                and refreshed_support_refs == lesson.support_refs
                and refreshed_based_on == lesson.based_on_review_ids
                and refreshed_contradictions == lesson.contradiction_refs
                and refreshed_sessions == lesson.support_session_ids
                and refreshed_queries == lesson.support_query_fingerprints
                and refreshed_query_fingerprints == lesson.query_fingerprints
                and latest_support == lesson.last_supported_at_utc
            ):
                continue
            now = _latest_timestamp(
                lesson.updated_at_utc,
                latest_support,
            )
            updated = replace(
                lesson,
                status=status,
                support_refs=refreshed_support_refs,
                based_on_review_ids=refreshed_based_on,
                contradiction_refs=refreshed_contradictions,
                support_session_ids=refreshed_sessions,
                support_query_fingerprints=refreshed_queries,
                query_fingerprints=refreshed_query_fingerprints,
                last_supported_at_utc=latest_support,
                updated_at_utc=now,
                status_changed_at_utc=now if status != lesson.status else lesson.status_changed_at_utc,
                version=lesson.version + 1,
            )
            self.store.save_lesson(
                updated,
                expected_version=lesson.version,
                idempotency_key=(
                    f"refresh:{lesson.lesson_id}:{decision.decision_id}"
                ),
            )

    def _quarantine_applied_lessons(self, decision: DecisionRecordV1) -> None:
        if not decision.applied_lesson_ids:
            return
        for lesson_id in decision.applied_lesson_ids:
            lesson = self.store.get_lesson(lesson_id)
            if lesson.status != "active":
                continue
            applied_reviews: list[tuple[DecisionRecordV1, OutcomeReviewV1]] = []
            for review in self.store.list_reviews():
                candidate = self.store.get_decision(review.decision_id)
                if lesson_id in candidate.applied_lesson_ids:
                    applied_reviews.append((candidate, review))
            proven_failure = next(
                (
                    review
                    for _candidate, review in applied_reviews
                    if review.has_safety_or_equivalence_failure
                ),
                None,
            )
            material_sessions = {
                candidate.session_id
                for candidate, review in applied_reviews
                if (
                    candidate.session_id
                    and review.complete
                    and review.material_regression_signal == "proven_failure"
                )
            }
            if proven_failure is None and len(material_sessions) < 2:
                continue
            audit_ref = (
                proven_failure.review_id
                if proven_failure is not None
                else next(
                    review.review_id
                    for _candidate, review in applied_reviews
                    if review.material_regression_signal == "proven_failure"
                )
            )
            self.store.transition_lesson(
                lesson_id,
                "quarantined",
                actor="service",
                idempotency_key=f"quarantine:{lesson_id}:{audit_ref}",
                audit_ref=audit_ref,
                at_utc=_latest_timestamp(lesson.updated_at_utc, self.clock()),
            )

    def list_learning_candidates(
        self,
        *,
        skill: str | None = None,
        learning_key: str | None = None,
        scope: Mapping[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        groups: dict[str, list[DecisionRecordV1]] = {}
        for decision in self.store.list_decisions():
            if skill is not None and decision.skill != skill:
                continue
            if learning_key is not None and decision.learning_key != learning_key:
                continue
            if scope is not None and any(
                self._decision_scope(decision).get(key) != value
                for key, value in scope.items()
                if key in _SCOPE_KEYS
            ):
                continue
            key = _canonical(
                {
                    "skill": decision.skill,
                    "skill_version": decision.skill_version,
                    "learning_key": decision.learning_key,
                    "scope": self._scope_key(decision),
                }
            )
            groups.setdefault(key, []).append(decision)
        candidates: list[dict[str, Any]] = []
        for decisions in groups.values():
            anchor = decisions[0]
            related = self._related_reviews(anchor)
            reviews = [review for _candidate, review in related]
            urgent = any(self._urgent(review) for review in reviews)
            existing = [
                lesson.lesson_id
                for lesson in self.store.list_lessons()
                if lesson.learning_key == anchor.learning_key
                and anchor.skill in lesson.applicable_skills
                and self._scope_compatible(
                    lesson,
                    self._decision_scope(anchor),
                )
            ]
            candidates.append(
                {
                    "skill": anchor.skill,
                    "skill_version": anchor.skill_version,
                    "learning_key": anchor.learning_key,
                    "scope": self._decision_scope(anchor),
                    "decision_count": len(decisions),
                    "complete_review_count": sum(review.complete for review in reviews),
                    "aligned_review_count": sum(
                        review.is_aligned_complete
                        and review.causal_strength in {"moderate", "strong"}
                        for review in reviews
                    ),
                    "session_count": len(
                        _dedupe(decision.session_id for decision in decisions)
                    ),
                    "query_fingerprint_count": len(
                        _dedupe(
                            decision.query_fingerprint for decision in decisions
                        )
                    ),
                    "subject_fingerprint_count": len(
                        _dedupe(
                            decision.subject_fingerprint for decision in decisions
                        )
                    ),
                    "candidate_kind": "urgent"
                    if urgent
                    else ("eligible" if self._eligible(related) else "developing"),
                    "review_ids": [review.review_id for review in reviews],
                    "existing_lesson_ids": sorted(existing),
                }
            )
        return sorted(
            candidates,
            key=lambda item: (
                {"urgent": 0, "eligible": 1, "developing": 2}[item["candidate_kind"]],
                item["skill"],
                item["learning_key"],
                _canonical(item["scope"]),
            ),
        )

    def recall(
        self,
        *,
        skill: str,
        skill_version: str,
        query_fingerprint: str | None,
        scope: Mapping[str, Any],
        runtime_compatibility_fingerprint: str,
        tool_schema_fingerprint: str | None = None,
        sanitized_config_fingerprint: str | None = None,
        tags: Iterable[str] = (),
        now_utc: str | None = None,
        max_results: int = 3,
    ) -> list[LessonV1]:
        if max_results <= 0:
            return []
        max_results = min(max_results, 3)
        now = parse_timestamp(now_utc or self.clock(), "now_utc")
        requested_scope = dict(scope)
        if (
            "skill_version" in requested_scope
            and requested_scope["skill_version"] != skill_version
        ):
            raise LearningServiceError("Requested skill version does not match its scope.")
        if (
            "runtime_compatibility_fingerprint" in requested_scope
            and requested_scope["runtime_compatibility_fingerprint"]
            != runtime_compatibility_fingerprint
        ):
            raise LearningServiceError(
                "Requested runtime compatibility fingerprint does not match its scope."
            )
        requested_scope["skill_version"] = skill_version
        requested_scope[
            "runtime_compatibility_fingerprint"
        ] = runtime_compatibility_fingerprint
        if tool_schema_fingerprint is not None:
            requested_scope["tool_schema_fingerprint"] = tool_schema_fingerprint
        if sanitized_config_fingerprint is not None:
            requested_scope["sanitized_config_fingerprint"] = sanitized_config_fingerprint
        requested_tags = set(tags)
        ranked: list[tuple[tuple[Any, ...], LessonV1]] = []
        for lesson in self.store.list_lessons(status="active"):
            if skill not in lesson.applicable_skills:
                continue
            if not self._scope_compatible(lesson, requested_scope):
                continue
            freshness_anchor = lesson.last_supported_at_utc or lesson.updated_at_utc
            age = now - parse_timestamp(freshness_anchor, "lesson freshness")
            if age < timedelta(0) or age > timedelta(
                days=min(self.freshness_days, lesson.freshness_days)
            ):
                continue
            compatible_scopes = [
                candidate
                for candidate in lesson.applicable_scopes
                if self._candidate_scope_matches(candidate, requested_scope)
            ]
            scope_specificity = max(
                (
                    (
                        int("database_fingerprint" in candidate),
                        int("server_fingerprint" in candidate),
                        len(candidate),
                    )
                    for candidate in compatible_scopes
                ),
                default=(0, 0, 0),
            )
            query_match = int(
                query_fingerprint is not None
                and query_fingerprint in lesson.query_fingerprints
            )
            tag_overlap = len(requested_tags.intersection(lesson.tags))
            evidence_strength = (
                len(lesson.support_refs)
                + len(lesson.support_session_ids)
                + len(lesson.support_query_fingerprints)
                - len(lesson.contradiction_refs)
            )
            ranked.append(
                (
                    (
                        -scope_specificity[0],
                        -scope_specificity[1],
                        -scope_specificity[2],
                        -query_match,
                        -tag_overlap,
                        -evidence_strength,
                        age.total_seconds(),
                        lesson.lesson_id,
                    ),
                    lesson,
                )
            )
        ranked.sort(key=lambda item: item[0])
        return [lesson for _rank, lesson in ranked[:max_results]]

    @staticmethod
    def _candidate_scope_matches(
        candidate: Mapping[str, Any],
        requested_scope: Mapping[str, Any],
    ) -> bool:
        if any(
            key not in candidate or candidate[key] != requested_scope[key]
            for key in ("skill_version", "runtime_compatibility_fingerprint")
            if key in requested_scope
        ):
            return False
        return all(
            key in requested_scope and requested_scope[key] == value
            for key, value in candidate.items()
            if key in _SCOPE_KEYS
        )

    @classmethod
    def _scope_compatible(
        cls,
        lesson: LessonV1,
        requested_scope: Mapping[str, Any],
    ) -> bool:
        return any(
            cls._candidate_scope_matches(candidate, requested_scope)
            for candidate in lesson.applicable_scopes
        )

    def record_terminal_link(self, **kwargs: Any) -> dict[str, Any]:
        return self.store.record_terminal_link(**kwargs)

    def terminal_link_exists(
        self,
        link_id: str,
        *,
        decision_id: str | None = None,
    ) -> bool:
        return self.store.terminal_link_exists(
            link_id,
            decision_id=decision_id,
        )

    def create_handoff(
        self,
        handoff: HandoffV1,
        *,
        idempotency_key: str | None = None,
    ) -> HandoffV1:
        return self.store.create_handoff(
            handoff,
            idempotency_key=idempotency_key,
        )

    def claim_handoff(
        self,
        handoff_id: str,
        owner: str,
        *,
        expected_version: int | None = None,
    ) -> HandoffV1:
        handoff = self.store.get_handoff(handoff_id)
        now = _latest_timestamp(handoff.updated_at_utc, self.clock())
        updated = replace(
            handoff,
            status="claimed",
            owner=owner,
            claimed_at_utc=now,
            updated_at_utc=now,
            version=handoff.version + 1,
        )
        return self.store.save_handoff(
            updated,
            expected_version=handoff.version
            if expected_version is None
            else expected_version,
        )

    def resolve_handoff(
        self,
        handoff_id: str,
        resolution: Mapping[str, Any],
        *,
        resolution_evidence_refs: Sequence[str] = (),
        expected_version: int | None = None,
    ) -> HandoffV1:
        handoff = self.store.get_handoff(handoff_id)
        if not resolution_evidence_refs and not resolution.get("human_decision"):
            raise LearningServiceError(
                "Resolving a handoff requires terminal evidence or human_decision=true."
            )
        for evidence_ref in resolution_evidence_refs:
            if evidence_ref.startswith("terminal-link-") and not self.store.terminal_link_exists(evidence_ref):
                raise LearningServiceError("Handoff resolution references an unknown terminal link.")
        now = _latest_timestamp(handoff.updated_at_utc, self.clock())
        updated = replace(
            handoff,
            status="resolved",
            resolution=dict(resolution),
            resolution_evidence_refs=tuple(resolution_evidence_refs),
            resolved_at_utc=now,
            updated_at_utc=now,
            version=handoff.version + 1,
        )
        return self.store.save_handoff(
            updated,
            expected_version=handoff.version
            if expected_version is None
            else expected_version,
        )

    def reopen_handoff(
        self,
        handoff_id: str,
        *,
        expected_version: int | None = None,
    ) -> HandoffV1:
        handoff = self.store.get_handoff(handoff_id)
        now = _latest_timestamp(handoff.updated_at_utc, self.clock())
        updated = replace(
            handoff,
            status="open",
            owner=None,
            claimed_at_utc=None,
            resolved_at_utc=None,
            cancelled_at_utc=None,
            resolution=None,
            reopen_count=handoff.reopen_count + 1,
            updated_at_utc=now,
            version=handoff.version + 1,
        )
        return self.store.save_handoff(
            updated,
            expected_version=handoff.version
            if expected_version is None
            else expected_version,
        )

    def cancel_handoff(
        self,
        handoff_id: str,
        *,
        expected_version: int | None = None,
    ) -> HandoffV1:
        handoff = self.store.get_handoff(handoff_id)
        now = _latest_timestamp(handoff.updated_at_utc, self.clock())
        updated = replace(
            handoff,
            status="cancelled",
            cancelled_at_utc=now,
            updated_at_utc=now,
            version=handoff.version + 1,
        )
        return self.store.save_handoff(
            updated,
            expected_version=handoff.version
            if expected_version is None
            else expected_version,
        )
