"""Per-session live preview ownership; callers hold the session state lock.

These methods never await. The chat runtime keeps database transitions and
stream changes under the same lock, including delayed stream registration.
"""

from dataclasses import dataclass, field
from uuid import UUID

from .stream import StreamSubscriber
from .types import TurnStart


@dataclass(slots=True)
class SessionStreams:
    turn_subscribers: dict[UUID, StreamSubscriber] = field(default_factory=dict)
    queued_subscribers: dict[UUID, StreamSubscriber] = field(default_factory=dict)
    active_turn_id: UUID | None = None
    active_preview_message_ids: frozenset[UUID] = field(default_factory=frozenset)

    def detach(self) -> tuple[StreamSubscriber, ...]:
        subscribers = (*self.turn_subscribers.values(), *self.queued_subscribers.values())
        self.turn_subscribers.clear()
        self.queued_subscribers.clear()
        self.active_turn_id = None
        self.active_preview_message_ids = frozenset()
        return subscribers

    def unregister(self, subscriber: StreamSubscriber) -> None:
        if self.queued_subscribers.get(subscriber.message_id) is subscriber:
            self.queued_subscribers.pop(subscriber.message_id, None)
        for turn_id, candidate in tuple(self.turn_subscribers.items()):
            if candidate is subscriber:
                self.turn_subscribers.pop(turn_id, None)

    def transfer(
        self,
        old_turn_id: UUID,
        new_turn: TurnStart,
    ) -> None:
        self.set_active_turn(new_turn, inherit_preview=True)
        candidates = [
            candidate
            for candidate in [
                self.turn_subscribers.pop(old_turn_id, None),
                self.turn_subscribers.pop(new_turn.turn_id, None),
                *self.take_queued(new_turn.message_ids),
            ]
            if candidate is not None and not candidate.closed
        ]
        if not candidates:
            return
        winner = max(candidates, key=lambda candidate: candidate.accepted_at)
        for candidate in candidates:
            if candidate is winner:
                continue
            candidate.send(
                {
                    "type": "stream_replaced",
                    "message_id": str(candidate.message_id),
                    "by_message_id": str(winner.message_id),
                }
            )
            candidate.close()
        self.turn_subscribers[new_turn.turn_id] = winner

    def close_turn(
        self,
        turn_id: UUID,
    ) -> None:
        subscriber = self.turn_subscribers.pop(turn_id, None)
        if subscriber is not None:
            subscriber.close()
        self.clear_active_turn(turn_id)

    def close_chain(self) -> None:
        subscribers = list(self.turn_subscribers.values())
        self.turn_subscribers.clear()
        self.active_turn_id = None
        self.active_preview_message_ids = frozenset()
        for subscriber in subscribers:
            subscriber.close()

    def close_queued(self) -> None:
        subscribers = tuple(self.queued_subscribers.values())
        self.queued_subscribers.clear()
        for subscriber in subscribers:
            subscriber.close()

    def adopt_running(
        self,
        turn_id: UUID,
    ) -> None:
        candidates = [
            candidate for candidate in self.turn_subscribers.values() if not candidate.closed
        ]
        self.turn_subscribers.clear()
        self.active_turn_id = turn_id
        if not candidates:
            return
        winner = candidates[0]
        for candidate in candidates[1:]:
            winner = self._replace_older(winner, candidate)
        self.turn_subscribers[turn_id] = winner

    def claim(
        self,
        turn: TurnStart,
    ) -> None:
        for subscriber in self.take_queued(turn.message_ids):
            self.install(
                turn_id=turn.turn_id,
                subscriber=subscriber,
            )

    def queue(
        self,
        subscriber: StreamSubscriber,
    ) -> None:
        current = self.queued_subscribers.get(subscriber.message_id)
        if current is None:
            self.queued_subscribers[subscriber.message_id] = subscriber
            return
        self.queued_subscribers[subscriber.message_id] = self._replace_older(
            current,
            subscriber,
        )

    def set_active_turn(
        self,
        turn: TurnStart,
        *,
        inherit_preview: bool,
    ) -> None:
        self.active_turn_id = turn.turn_id
        if turn.message_ids:
            self.active_preview_message_ids = frozenset(turn.message_ids)
        elif not inherit_preview:
            self.active_preview_message_ids = frozenset()

    def clear_active_turn(self, turn_id: UUID) -> None:
        if self.active_turn_id != turn_id:
            return
        self.active_turn_id = None
        self.active_preview_message_ids = frozenset()

    def take_queued(
        self,
        message_ids: tuple[UUID, ...],
    ) -> list[StreamSubscriber]:
        return [
            subscriber
            for message_id in message_ids
            if (subscriber := self.queued_subscribers.pop(message_id, None)) is not None
            and not subscriber.closed
        ]

    def install(
        self,
        *,
        turn_id: UUID,
        subscriber: StreamSubscriber,
    ) -> None:
        current = self.turn_subscribers.get(turn_id)
        if current is None:
            self.turn_subscribers[turn_id] = subscriber
            return
        winner = self._replace_older(current, subscriber)
        self.turn_subscribers[turn_id] = winner

    @staticmethod
    def _replace_older(
        left: StreamSubscriber,
        right: StreamSubscriber,
    ) -> StreamSubscriber:
        winner, loser = (right, left) if left.accepted_at <= right.accepted_at else (left, right)
        loser.send(
            {
                "type": "stream_replaced",
                "message_id": str(loser.message_id),
                "by_message_id": str(winner.message_id),
            }
        )
        loser.close()
        return winner
