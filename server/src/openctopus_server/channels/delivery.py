from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal

from .types import DeliveryPlan

MAX_DELIVERY_ACTIONS = 32

ActionResultStatus = Literal["sent", "failed", "unknown"]
ActionFinalStatus = Literal["sent", "failed", "unknown", "skipped"]
DeliveryAggregateStatus = Literal["sent", "partial", "failed", "unknown"]


class DeliveryPlanTooLargeError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class ActionResult:
    status: ActionResultStatus
    platform_message_id: str | None = None
    artifact_id: str | None = None
    error_code: str | None = None
    error_message: str | None = None


@dataclass(frozen=True, slots=True)
class DeliveryAggregate:
    status: DeliveryAggregateStatus
    action_statuses: tuple[ActionFinalStatus, ...]
    visible_sent_actions: int
    visible_total_actions: int


def split_text_actions(
    content: str,
    *,
    max_chars: int,
    max_actions: int = MAX_DELIVERY_ACTIONS,
) -> tuple[str, ...]:
    """Split text without loss, preferring newline, then whitespace boundaries."""
    if not content:
        raise ValueError("Delivery text must not be empty")
    if max_chars <= 0:
        raise ValueError("Text action limit must be positive")
    if max_actions <= 0:
        raise ValueError("Delivery action limit must be positive")

    remaining = content
    chunks: list[str] = []
    while remaining:
        if len(chunks) == max_actions:
            raise DeliveryPlanTooLargeError(
                f"Delivery plan exceeds the {max_actions}-action limit"
            )
        if len(remaining) <= max_chars:
            chunks.append(remaining)
            break

        candidate = remaining[:max_chars]
        split_at = candidate.rfind("\n")
        if split_at < 0:
            split_at = _last_whitespace(candidate)
        boundary = split_at + 1 if split_at >= 0 else max_chars
        chunks.append(remaining[:boundary])
        remaining = remaining[boundary:]

    return tuple(chunks)


def aggregate_action_results(
    plan: DeliveryPlan,
    results: Sequence[ActionResult],
) -> DeliveryAggregate:
    """Produce one terminal delivery result and mark an unattempted tail skipped."""
    actions = plan.actions
    if not actions:
        raise ValueError("Delivery plan must contain at least one action")
    if len(actions) > MAX_DELIVERY_ACTIONS:
        raise DeliveryPlanTooLargeError(
            f"Delivery plan exceeds the {MAX_DELIVERY_ACTIONS}-action limit"
        )
    if len(results) > len(actions):
        raise ValueError("Action results exceed the delivery plan")

    statuses: list[ActionFinalStatus] = []
    terminal_failure: Literal["failed", "unknown"] | None = None
    for index, result in enumerate(results):
        if result.status not in {"sent", "failed", "unknown"}:
            raise ValueError("Unknown delivery action result")
        if terminal_failure is not None:
            raise ValueError("Action result exists after a terminal failure")
        statuses.append(result.status)
        if result.status in {"failed", "unknown"}:
            terminal_failure = result.status
            if index != len(results) - 1:
                raise ValueError("Action result exists after a terminal failure")

    if terminal_failure is None and len(statuses) != len(actions):
        raise ValueError("Delivery requires a terminal action result")

    statuses.extend("skipped" for _ in range(len(actions) - len(statuses)))
    visible_total = sum(action.visible for action in actions)
    visible_sent = sum(
        action.visible and status == "sent"
        for action, status in zip(actions, statuses, strict=True)
    )

    if terminal_failure is None:
        aggregate_status: DeliveryAggregateStatus = "sent"
    elif visible_sent:
        aggregate_status = "partial"
    else:
        aggregate_status = terminal_failure

    return DeliveryAggregate(
        status=aggregate_status,
        action_statuses=tuple(statuses),
        visible_sent_actions=visible_sent,
        visible_total_actions=visible_total,
    )


def _last_whitespace(value: str) -> int:
    for index in range(len(value) - 1, -1, -1):
        if value[index].isspace():
            return index
    return -1
