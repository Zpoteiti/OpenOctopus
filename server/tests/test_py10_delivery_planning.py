from uuid import UUID

import pytest

from openctopus_server.channels.adapters.discord import plan_discord_text_delivery
from openctopus_server.channels.delivery import (
    MAX_DELIVERY_ACTIONS,
    ActionResult,
    DeliveryPlanTooLargeError,
    aggregate_action_results,
)
from openctopus_server.channels.types import (
    DeliveryAction,
    DeliveryPlan,
    OutboundMessage,
)

USER_ID = UUID("10000000-0000-4000-8000-000000000001")
BINDING_GENERATION = UUID("20000000-0000-4000-8000-000000000001")


def _message(content: str) -> OutboundMessage:
    return OutboundMessage(
        delivery_key="delivery-key",
        user_id=USER_ID,
        turn_id=None,
        origin="final",
        channel="discord",
        chat_id="123456789",
        binding_generation=BINDING_GENERATION,
        content=content,
    )


def _text_action(content: str) -> DeliveryAction:
    return DeliveryAction(kind="text_message", visible=True, content=content)


def test_discord_text_at_limit_stays_in_one_action() -> None:
    content = "x" * 2_000

    plan = plan_discord_text_delivery(_message(content))

    assert len(plan.actions) == 1
    assert plan.actions[0].content == content
    assert plan.actions[0].chat_id == "123456789"
    assert plan.actions[0].idempotency_key is not None


def test_discord_split_prefers_newline_over_later_space_without_losing_text() -> None:
    content = "a" * 1_500 + "\n" + "b" * 400 + " " + "c" * 200

    plan = plan_discord_text_delivery(_message(content))

    assert plan.actions[0].content == "a" * 1_500 + "\n"
    assert "".join(action.content or "" for action in plan.actions) == content
    assert all(len(action.content or "") <= 2_000 for action in plan.actions)


def test_discord_split_falls_back_to_whitespace_then_hard_unicode_code_points() -> None:
    whitespace_content = "a" * 1_500 + "\t" + "b" * 700
    unicode_content = "🙂" * 2_001

    whitespace_plan = plan_discord_text_delivery(_message(whitespace_content))
    unicode_plan = plan_discord_text_delivery(_message(unicode_content))

    assert whitespace_plan.actions[0].content == "a" * 1_500 + "\t"
    assert "".join(action.content or "" for action in whitespace_plan.actions) == whitespace_content
    assert [len(action.content or "") for action in unicode_plan.actions] == [2_000, 1]
    assert "".join(action.content or "" for action in unicode_plan.actions) == unicode_content


def test_discord_split_preserves_markdown_and_all_whitespace() -> None:
    content = ("**heading**\n" + "word " * 500 + "\n```text\nvalue\n```\n") * 3

    plan = plan_discord_text_delivery(_message(content))

    assert "".join(action.content or "" for action in plan.actions) == content
    assert all(action.kind == "text_message" and action.visible for action in plan.actions)


def test_discord_plan_accepts_exact_action_bound_and_rejects_overflow() -> None:
    exact = plan_discord_text_delivery(_message("x" * (2_000 * MAX_DELIVERY_ACTIONS)))

    assert len(exact.actions) == MAX_DELIVERY_ACTIONS
    with pytest.raises(DeliveryPlanTooLargeError):
        plan_discord_text_delivery(_message("x" * (2_000 * MAX_DELIVERY_ACTIONS + 1)))


def test_aggregate_all_sent() -> None:
    plan = DeliveryPlan(actions=(_text_action("one"), _text_action("two")))

    aggregate = aggregate_action_results(
        plan,
        (ActionResult(status="sent"), ActionResult(status="sent")),
    )

    assert aggregate.status == "sent"
    assert aggregate.action_statuses == ("sent", "sent")
    assert aggregate.visible_sent_actions == 2
    assert aggregate.visible_total_actions == 2


@pytest.mark.parametrize("terminal", ["failed", "unknown"])
def test_aggregate_partial_stops_and_skips_tail_after_visible_send(terminal: str) -> None:
    plan = DeliveryPlan(
        actions=(_text_action("one"), _text_action("two"), _text_action("three"))
    )

    aggregate = aggregate_action_results(
        plan,
        (ActionResult(status="sent"), ActionResult(status=terminal)),
    )

    assert aggregate.status == "partial"
    assert aggregate.action_statuses == ("sent", terminal, "skipped")
    assert aggregate.visible_sent_actions == 1
    assert aggregate.visible_total_actions == 3


@pytest.mark.parametrize("terminal", ["failed", "unknown"])
def test_aggregate_zero_visible_sends_preserves_terminal_status(terminal: str) -> None:
    plan = DeliveryPlan(actions=(_text_action("one"), _text_action("two")))

    aggregate = aggregate_action_results(plan, (ActionResult(status=terminal),))

    assert aggregate.status == terminal
    assert aggregate.action_statuses == (terminal, "skipped")
    assert aggregate.visible_sent_actions == 0


def test_invisible_upload_does_not_make_visible_failure_partial() -> None:
    plan = DeliveryPlan(
        actions=(
            DeliveryAction(kind="file_upload", visible=False, media_index=0),
            DeliveryAction(kind="file_message", visible=True, media_index=0),
        )
    )

    aggregate = aggregate_action_results(
        plan,
        (ActionResult(status="sent"), ActionResult(status="failed")),
    )

    assert aggregate.status == "failed"
    assert aggregate.visible_sent_actions == 0
    assert aggregate.action_statuses == ("sent", "failed")


def test_aggregate_rejects_nonterminal_or_post_failure_results() -> None:
    plan = DeliveryPlan(actions=(_text_action("one"), _text_action("two")))

    with pytest.raises(ValueError, match="terminal action result"):
        aggregate_action_results(plan, (ActionResult(status="sent"),))
    with pytest.raises(ValueError, match="after a terminal failure"):
        aggregate_action_results(
            plan,
            (ActionResult(status="failed"), ActionResult(status="sent")),
        )
