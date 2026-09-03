from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import delete, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.db.models import (
    ChannelDelivery,
    ChannelDeliveryAction,
    ChannelMessageReceipt,
    DingTalkConfig,
    DiscordConfig,
    Message,
    PendingMessage,
    Session,
    TurnRun,
    User,
)


async def _owner_and_session(db: AsyncSession) -> tuple[User, Session]:
    owner = User(
        id=uuid4(),
        email=f"{uuid4()}@test.com",
        password_hash="hash",
        name="Owner",
    )
    session = Session(
        id=uuid4(),
        user_id=owner.id,
        session_key=f"discord:application-1:{uuid4()}",
        channel="discord",
        chat_id="channel-1",
        title="Channel",
    )
    db.add(owner)
    await db.flush()
    db.add(session)
    await db.flush()
    return owner, session


async def test_channel_configs_persist_pairing_and_rotation_identity(pg_engine) -> None:
    now = datetime.now(UTC)
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        owner, _ = await _owner_and_session(db)
        discord = DiscordConfig(
            user_id=owner.id,
            bot_token="discord-secret",
            application_id="application-1",
            bot_user_id="bot-1",
            bot_display_name="Bob",
            bot_avatar_url=None,
            binding_generation=uuid4(),
            revision=2,
            owner_platform_user_id="owner-1",
            owner_dm_chat_id="dm-1",
            paired_at=now,
            allow_list=["42"],
            pairing_code_hash=None,
            pairing_expires_at=None,
            created_at=now,
            updated_at=now,
        )
        dingtalk = DingTalkConfig(
            user_id=owner.id,
            client_id="client-1",
            client_secret="dingtalk-secret",
            bot_user_id="ding-bot-1",
            bot_display_name="Ding Bob",
            bot_avatar_url=None,
            binding_generation=uuid4(),
            revision=1,
            owner_platform_user_id=None,
            owner_dm_chat_id=None,
            paired_at=None,
            allow_list=[],
            pairing_code_hash=b"x" * 32,
            pairing_expires_at=now + timedelta(minutes=10),
            created_at=now,
            updated_at=now,
        )
        db.add_all([discord, dingtalk])
        await db.commit()

    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        stored_discord = await db.get(DiscordConfig, owner.id)
        stored_dingtalk = await db.get(DingTalkConfig, owner.id)

    assert stored_discord is not None
    assert stored_discord.application_id == "application-1"
    assert stored_discord.owner_dm_chat_id == "dm-1"
    assert stored_discord.allow_list == ["42"]
    assert stored_dingtalk is not None
    assert stored_dingtalk.client_id == "client-1"
    assert stored_dingtalk.pairing_code_hash == b"x" * 32


async def test_bot_identity_is_unique_across_users(pg_engine) -> None:
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        first, _ = await _owner_and_session(db)
        second, _ = await _owner_and_session(db)
        common = {
            "bot_token": "secret",
            "application_id": "same-application",
            "bot_user_id": "bot",
            "bot_display_name": "Bot",
            "binding_generation": uuid4(),
        }
        db.add(DiscordConfig(user_id=first.id, **common))
        await db.commit()

    with pytest.raises(IntegrityError):
        async with AsyncSession(pg_engine, expire_on_commit=False) as db:
            db.add(DiscordConfig(user_id=second.id, **common))
            await db.commit()


async def test_receipt_survives_session_delete_and_keeps_dedup_identity(pg_engine) -> None:
    binding_generation = uuid4()
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        owner, session = await _owner_and_session(db)
        receipt = ChannelMessageReceipt(
            id=uuid4(),
            user_id=owner.id,
            session_id=session.id,
            channel="discord",
            binding_generation=binding_generation,
            chat_id=session.chat_id,
            source_message_id="message-1",
            disposition="trigger",
        )
        db.add(receipt)
        await db.commit()
        receipt_id = receipt.id
        owner_id = owner.id
        session_id = session.id

    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        await db.execute(delete(Session).where(Session.id == session_id))
        await db.commit()
        stored = await db.get(ChannelMessageReceipt, receipt_id)

    assert stored is not None
    assert stored.session_id is None

    with pytest.raises(IntegrityError):
        async with AsyncSession(pg_engine, expire_on_commit=False) as db:
            db.add(
                ChannelMessageReceipt(
                    id=uuid4(),
                    user_id=owner_id,
                    session_id=None,
                    channel="discord",
                    binding_generation=binding_generation,
                    chat_id="channel-1",
                    source_message_id="message-1",
                    disposition="context",
                )
            )
            await db.commit()


async def test_structured_sender_profile_and_turn_inputs_round_trip(pg_engine) -> None:
    now = datetime.now(UTC)
    binding_generation = uuid4()
    pending_id = uuid4()
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        owner, session = await _owner_and_session(db)
        pending = PendingMessage(
            id=pending_id,
            session_id=session.id,
            user_id=owner.id,
            session_key=session.session_key,
            content=[{"type": "text", "text": "hello"}],
            attachment_refs=[],
            effort=None,
            sender_id="42",
            sender_display_name="Colleague",
            sender_classification="allowed_non_owner",
            ingress_tool_profile="message_only",
            source_message_id="message-1",
            channel_binding_generation=binding_generation,
            channel_context=[{"source_message_id": "prior-1", "text": "context"}],
            received_at=now,
        )
        message = Message(
            id=pending_id,
            session_id=session.id,
            message_kind="human",
            content=[{"type": "text", "text": "hello"}],
            attachment_refs=[],
            delivery_refs=[],
            sender_id="42",
            sender_display_name="Colleague",
            sender_classification="allowed_non_owner",
            ingress_tool_profile="message_only",
            source_message_id="message-1",
            channel_binding_generation=binding_generation,
            channel_context=[{"source_message_id": "prior-1", "text": "context"}],
            created_at=now,
        )
        turn = TurnRun(
            id=uuid4(),
            session_id=session.id,
            runner_instance_id=uuid4(),
            status="running",
            tool_profile="message_only",
            input_message_ids=[str(pending_id)],
            failed_delivery_targets=[
                {
                    "channel": "discord",
                    "chat_id": session.chat_id,
                    "binding_generation": str(binding_generation),
                }
            ],
            started_at=now,
        )
        db.add_all([pending, message, turn])
        await db.commit()
        turn_id = turn.id

    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        stored_pending = await db.get(PendingMessage, pending_id)
        stored_message = await db.get(Message, pending_id)
        stored_turn = await db.get(TurnRun, turn_id)

    assert stored_pending is not None
    assert stored_pending.ingress_tool_profile == "message_only"
    assert stored_pending.channel_context[0]["text"] == "context"
    assert stored_message is not None
    assert stored_message.sender_classification == "allowed_non_owner"
    assert stored_turn is not None
    assert stored_turn.tool_profile == "message_only"
    assert stored_turn.input_message_ids == [str(pending_id)]
    assert stored_turn.failed_delivery_targets == [
        {
            "channel": "discord",
            "chat_id": "channel-1",
            "binding_generation": str(binding_generation),
        }
    ]


async def test_invalid_authority_values_are_rejected(pg_engine) -> None:
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        owner, session = await _owner_and_session(db)
        db.add(
            PendingMessage(
                id=uuid4(),
                session_id=session.id,
                user_id=owner.id,
                session_key=session.session_key,
                content=[],
                sender_id="42",
                sender_display_name=None,
                sender_classification="owner",
                ingress_tool_profile="unrestricted",
                source_message_id="message-1",
                channel_binding_generation=uuid4(),
                channel_context=[],
            )
        )
        with pytest.raises(IntegrityError):
            await db.commit()


async def test_delivery_status_is_separate_from_message_delivery_refs(pg_engine) -> None:
    now = datetime.now(UTC)
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        owner, session = await _owner_and_session(db)
        assistant = Message(
            id=uuid4(),
            session_id=session.id,
            message_kind="assistant",
            content=[{"type": "text", "text": "answer"}],
            attachment_refs=[],
            delivery_refs=[{"type": "workspace_file", "filename": "report.pdf"}],
            channel_context=[],
            created_at=now,
        )
        turn = TurnRun(
            id=uuid4(),
            session_id=session.id,
            runner_instance_id=uuid4(),
            status="completed",
            tool_profile="owner_full",
            input_message_ids=[],
            started_at=now,
            finished_at=now,
        )
        db.add_all([assistant, turn])
        await db.flush()
        delivery = ChannelDelivery(
            id=uuid4(),
            user_id=owner.id,
            session_id=session.id,
            turn_id=turn.id,
            assistant_message_id=assistant.id,
            tool_use_id=None,
            delivery_key=f"final:{assistant.id}",
            origin="final",
            channel="discord",
            chat_id=session.chat_id,
            binding_generation=uuid4(),
            status="attempting",
            total_actions=1,
            visible_sent_actions=0,
            created_at=now,
            started_at=now,
        )
        db.add(delivery)
        await db.flush()
        action = ChannelDeliveryAction(
            id=uuid4(),
            delivery_id=delivery.id,
            action_index=0,
            action_kind="file_message",
            visible=True,
            status="attempting",
            started_at=now,
        )
        db.add(action)
        await db.commit()
        delivery_id = delivery.id

    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        stored_delivery = await db.get(ChannelDelivery, delivery_id)
        stored_assistant = await db.scalar(
            select(Message).where(Message.id == assistant.id)
        )

    assert stored_delivery is not None
    assert stored_delivery.status == "attempting"
    assert stored_assistant is not None
    assert stored_assistant.delivery_refs == [
        {"type": "workspace_file", "filename": "report.pdf"}
    ]
