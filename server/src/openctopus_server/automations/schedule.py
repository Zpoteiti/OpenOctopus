from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Literal, cast
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from croniter import (  # type: ignore[import-untyped]
    CroniterBadCronError,
    CroniterBadDateError,
    croniter,
)

MIN_EVERY_SECONDS = 60
MAX_EVERY_SECONDS = 31_536_000
MAX_CRON_EXPRESSION_CHARS = 256
MAX_TIMEZONE_CHARS = 64

ScheduleKind = Literal["every", "cron", "at"]

_MONTH_NAMES = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}
_WEEKDAY_NAMES = {
    "SUN": 0,
    "MON": 1,
    "TUE": 2,
    "WED": 3,
    "THU": 4,
    "FRI": 5,
    "SAT": 6,
}
_DECIMAL = re.compile(r"^[0-9]+$")
_NAME = re.compile(r"^[A-Za-z]{3}$")


class InvalidScheduleError(ValueError):
    pass


class InvalidTimezoneError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class ScheduleSpec:
    kind: ScheduleKind
    value: str
    timezone: str | None
    next_fire_at: datetime


def validate_timezone_name(name: str) -> str:
    if not isinstance(name, str):
        raise InvalidTimezoneError("Timezone must be an IANA name")
    if not 1 <= len(name) <= MAX_TIMEZONE_CHARS or name != name.strip():
        raise InvalidTimezoneError("Timezone must be an IANA name")
    if name != "UTC" and ("/" not in name or name.startswith("Etc/GMT")):
        raise InvalidTimezoneError("Timezone must be an IANA name")
    try:
        zone = ZoneInfo(name)
    except (ValueError, ZoneInfoNotFoundError) as exc:
        raise InvalidTimezoneError("Timezone must be an IANA name") from exc
    return zone.key


def canonicalize_schedule(
    *,
    every_seconds: int | None,
    cron_expr: str | None,
    at: str | datetime | None,
    timezone: str | None,
    user_timezone: str,
    now: datetime,
) -> ScheduleSpec:
    now_utc = _as_utc(now)
    timing_count = sum(value is not None for value in (every_seconds, cron_expr, at))
    if timing_count != 1:
        raise InvalidScheduleError("Exactly one schedule form is required")

    if every_seconds is not None:
        if timezone is not None:
            raise InvalidScheduleError("Timezone is not valid for an interval schedule")
        if (
            isinstance(every_seconds, bool)
            or not isinstance(every_seconds, int)
            or not MIN_EVERY_SECONDS <= every_seconds <= MAX_EVERY_SECONDS
        ):
            raise InvalidScheduleError("Interval seconds are out of range")
        return ScheduleSpec(
            kind="every",
            value=str(every_seconds),
            timezone=None,
            next_fire_at=now_utc + timedelta(seconds=every_seconds),
        )

    if cron_expr is not None:
        if not isinstance(cron_expr, str):
            raise InvalidScheduleError("Cron expression must be a string")
        expression = _canonical_cron_expression(cron_expr)
        effective_timezone = validate_timezone_name(
            user_timezone if timezone is None else timezone
        )
        next_fire_at = _next_cron(expression, effective_timezone, now_utc)
        return ScheduleSpec(
            kind="cron",
            value=expression,
            timezone=effective_timezone,
            next_fire_at=next_fire_at,
        )

    assert at is not None
    parsed = _parse_datetime(at)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        effective_timezone = validate_timezone_name(
            user_timezone if timezone is None else timezone
        )
        candidates = _local_utc_candidates(parsed, ZoneInfo(effective_timezone))
        if len(candidates) != 1:
            raise InvalidScheduleError("Local one-shot time is nonexistent or ambiguous")
        instant = candidates[0]
    else:
        instant = parsed.astimezone(UTC)
        if timezone is None:
            effective_timezone = "UTC"
        else:
            effective_timezone = validate_timezone_name(timezone)
            effective_offset = instant.astimezone(ZoneInfo(effective_timezone)).utcoffset()
            if effective_offset != parsed.utcoffset():
                raise InvalidScheduleError("One-shot offset does not match timezone")
    if instant <= now_utc:
        raise InvalidScheduleError("One-shot time must be in the future")
    return ScheduleSpec(
        kind="at",
        value=_rfc3339_utc(instant),
        timezone=effective_timezone,
        next_fire_at=instant,
    )


def advance_recurring(
    schedule: ScheduleSpec,
    *,
    scheduled_at: datetime,
    now: datetime,
) -> datetime:
    now_utc = _as_utc(now)
    boundary = _as_utc(scheduled_at)
    if schedule.kind == "every":
        seconds = int(schedule.value)
        elapsed = max(0.0, (now_utc - boundary).total_seconds())
        steps = int(elapsed // seconds) + 1
        return boundary + timedelta(seconds=steps * seconds)
    if schedule.kind == "cron":
        if schedule.timezone is None:
            raise InvalidScheduleError("Stored cron timezone is missing")
        return _next_cron(schedule.value, schedule.timezone, now_utc)
    raise InvalidScheduleError("One-shot schedules do not recur")


def latest_due_occurrence(
    schedule: ScheduleSpec,
    *,
    scheduled_at: datetime,
    now: datetime,
) -> datetime:
    """Return the most recent due boundary without inventing catch-up runs."""
    now_utc = _as_utc(now)
    boundary = _as_utc(scheduled_at)
    if boundary > now_utc:
        raise InvalidScheduleError("Schedule is not due")
    if schedule.kind == "every":
        seconds = int(schedule.value)
        elapsed = (now_utc - boundary).total_seconds()
        return boundary + timedelta(seconds=int(elapsed // seconds) * seconds)
    if schedule.kind == "cron":
        if schedule.timezone is None:
            raise InvalidScheduleError("Stored cron timezone is missing")
        candidate = _previous_cron(schedule.value, schedule.timezone, now_utc)
        return max(boundary, candidate)
    return boundary


def schedule_from_storage(
    *,
    kind: str,
    value: str,
    timezone: str | None,
    next_fire_at: datetime,
) -> ScheduleSpec:
    if kind not in {"every", "cron", "at"}:
        raise InvalidScheduleError("Stored schedule kind is invalid")
    if kind == "every":
        if timezone is not None:
            raise InvalidScheduleError("Stored interval timezone is invalid")
        try:
            seconds = int(value)
        except ValueError as exc:
            raise InvalidScheduleError("Stored interval is invalid") from exc
        if str(seconds) != value or not MIN_EVERY_SECONDS <= seconds <= MAX_EVERY_SECONDS:
            raise InvalidScheduleError("Stored interval is invalid")
    elif kind == "cron":
        if timezone is None:
            raise InvalidScheduleError("Stored cron timezone is missing")
        _canonical_cron_expression(value)
        validate_timezone_name(timezone)
    else:
        if timezone is None:
            raise InvalidScheduleError("Stored one-shot timezone is missing")
        parsed = _parse_datetime(value)
        if parsed.tzinfo is None or parsed.astimezone(UTC) != _as_utc(next_fire_at):
            raise InvalidScheduleError("Stored one-shot instant is invalid")
        validate_timezone_name(timezone)
    return ScheduleSpec(
        kind=cast(ScheduleKind, kind),
        value=value,
        timezone=timezone,
        next_fire_at=_as_utc(next_fire_at),
    )


def _canonical_cron_expression(raw: str) -> str:
    expression = " ".join(raw.strip().split())
    if not expression or len(expression) > MAX_CRON_EXPRESSION_CHARS:
        raise InvalidScheduleError("Cron expression is invalid")
    fields = expression.split(" ")
    if len(fields) != 5:
        raise InvalidScheduleError("Cron expression must have five fields")
    bounds = ((0, 59), (0, 23), (1, 31), (1, 12), (0, 7))
    names = (None, None, None, _MONTH_NAMES, _WEEKDAY_NAMES)
    for field, (minimum, maximum), field_names in zip(fields, bounds, names, strict=True):
        _validate_cron_field(field, minimum, maximum, field_names)
    try:
        croniter(
            expression,
            datetime(2000, 1, 1),
            ret_type=datetime,
            day_or=True,
            max_years_between_matches=50,
        )
    except (CroniterBadCronError, KeyError, ValueError) as exc:
        raise InvalidScheduleError("Cron expression is invalid") from exc
    return expression


def _validate_cron_field(
    field: str,
    minimum: int,
    maximum: int,
    names: dict[str, int] | None,
) -> None:
    for item in field.split(","):
        if not item:
            raise InvalidScheduleError("Cron expression is invalid")
        pieces = item.split("/")
        if len(pieces) > 2:
            raise InvalidScheduleError("Cron expression is invalid")
        base = pieces[0]
        if len(pieces) == 2:
            step = pieces[1]
            if not _DECIMAL.fullmatch(step) or int(step) <= 0:
                raise InvalidScheduleError("Cron step is invalid")
        if base == "*":
            continue
        endpoints = base.split("-")
        if len(endpoints) > 2:
            raise InvalidScheduleError("Cron range is invalid")
        values = [_cron_atom_value(atom, minimum, maximum, names) for atom in endpoints]
        if len(values) == 2 and values[0] > values[1]:
            raise InvalidScheduleError("Cron range is invalid")


def _cron_atom_value(
    atom: str,
    minimum: int,
    maximum: int,
    names: dict[str, int] | None,
) -> int:
    if _DECIMAL.fullmatch(atom):
        value = int(atom)
    elif names is not None and _NAME.fullmatch(atom):
        try:
            value = names[atom.upper()]
        except KeyError as exc:
            raise InvalidScheduleError("Cron name is invalid") from exc
    else:
        raise InvalidScheduleError("Cron field is invalid")
    if not minimum <= value <= maximum:
        raise InvalidScheduleError("Cron field is out of range")
    return value


def _next_cron(expression: str, timezone: str, now: datetime) -> datetime:
    zone = ZoneInfo(validate_timezone_name(timezone))
    now_utc = _as_utc(now)
    base_local = now_utc.astimezone(zone).replace(tzinfo=None)
    try:
        iterator = croniter(
            expression,
            base_local,
            ret_type=datetime,
            day_or=True,
            max_years_between_matches=50,
        )
        for _ in range(10_000):
            local_candidate = iterator.get_next(datetime)
            candidates = _local_utc_candidates(local_candidate, zone)
            if not candidates:
                continue
            earlier = candidates[0]
            if earlier > now_utc:
                return earlier
    except (CroniterBadCronError, CroniterBadDateError, OverflowError, ValueError) as exc:
        raise InvalidScheduleError("Cron expression has no future occurrence") from exc
    raise InvalidScheduleError("Cron expression has no future occurrence")


def _previous_cron(expression: str, timezone: str, now: datetime) -> datetime:
    zone = ZoneInfo(validate_timezone_name(timezone))
    now_utc = _as_utc(now)
    base_local = now_utc.astimezone(zone).replace(tzinfo=None) + timedelta(microseconds=1)
    try:
        previous_iterator = croniter(
            expression,
            base_local,
            ret_type=datetime,
            day_or=True,
            max_years_between_matches=50,
        )
        previous: datetime | None = None
        for _ in range(10_000):
            local_candidate = previous_iterator.get_prev(datetime)
            candidates = _local_utc_candidates(local_candidate, zone)
            if not candidates:
                continue
            earlier = candidates[0]
            if earlier <= now_utc:
                previous = earlier
                break

        # During a fall-back fold, a later local wall time may already have
        # occurred in the earlier fold even though it is still ahead of the
        # current repeated wall time. Check those forward wall-time candidates
        # until their earlier-fold instant is genuinely in the future.
        forward_iterator = croniter(
            expression,
            base_local,
            ret_type=datetime,
            day_or=True,
            max_years_between_matches=50,
        )
        for _ in range(10_000):
            local_candidate = forward_iterator.get_next(datetime)
            candidates = _local_utc_candidates(local_candidate, zone)
            if not candidates:
                continue
            earlier = candidates[0]
            if earlier > now_utc:
                break
            if previous is None or earlier > previous:
                previous = earlier
        if previous is not None:
            return previous
    except (CroniterBadCronError, CroniterBadDateError, OverflowError, ValueError) as exc:
        raise InvalidScheduleError("Cron expression has no previous occurrence") from exc
    raise InvalidScheduleError("Cron expression has no previous occurrence")


def _local_utc_candidates(local: datetime, zone: ZoneInfo) -> list[datetime]:
    if local.tzinfo is not None:
        raise InvalidScheduleError("Expected a naive local datetime")
    candidates: set[datetime] = set()
    for fold in (0, 1):
        candidate = local.replace(tzinfo=zone, fold=fold).astimezone(UTC)
        round_trip = candidate.astimezone(zone).replace(tzinfo=None)
        if round_trip == local:
            candidates.add(candidate)
    return sorted(candidates)


def _parse_datetime(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str) or not value or value != value.strip():
        raise InvalidScheduleError("One-shot time is invalid")
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise InvalidScheduleError("One-shot time is invalid") from exc


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise InvalidScheduleError("UTC datetime must be timezone-aware")
    return value.astimezone(UTC)


def _rfc3339_utc(value: datetime) -> str:
    return _as_utc(value).isoformat().replace("+00:00", "Z")
