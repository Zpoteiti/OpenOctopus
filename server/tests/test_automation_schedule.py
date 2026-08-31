from datetime import UTC, datetime

import pytest

from openctopus_server.automations.schedule import (
    InvalidScheduleError,
    InvalidTimezoneError,
    ScheduleSpec,
    advance_recurring,
    canonicalize_schedule,
    latest_due_occurrence,
    validate_timezone_name,
)

NOW = datetime(2026, 8, 31, 12, 0, tzinfo=UTC)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("UTC", "UTC"),
        ("Asia/Shanghai", "Asia/Shanghai"),
        ("America/New_York", "America/New_York"),
    ],
)
def test_validate_timezone_name_accepts_iana_names(value: str, expected: str) -> None:
    assert validate_timezone_name(value) == expected


@pytest.mark.parametrize(
    "value",
    ["", " UTC", "UTC ", "CST", "+08:00", "Etc/GMT+8", "Not/A_Zone", "a" * 65],
)
def test_validate_timezone_name_rejects_invalid_or_fixed_offset_names(value: str) -> None:
    with pytest.raises(InvalidTimezoneError):
        validate_timezone_name(value)


@pytest.mark.parametrize("seconds", [60, 31_536_000])
def test_every_schedule_accepts_bounds(seconds: int) -> None:
    schedule = canonicalize_schedule(
        every_seconds=seconds,
        cron_expr=None,
        at=None,
        timezone=None,
        user_timezone="Asia/Shanghai",
        now=NOW,
    )

    assert schedule == ScheduleSpec(
        kind="every",
        value=str(seconds),
        timezone=None,
        next_fire_at=datetime.fromtimestamp(NOW.timestamp() + seconds, tz=UTC),
    )


@pytest.mark.parametrize("seconds", [True, False, 0, 59, 31_536_001])
def test_every_schedule_rejects_bool_and_out_of_range(seconds: object) -> None:
    with pytest.raises(InvalidScheduleError):
        canonicalize_schedule(
            every_seconds=seconds,
            cron_expr=None,
            at=None,
            timezone=None,
            user_timezone="UTC",
            now=NOW,
        )


def test_schedule_requires_exactly_one_timing_form() -> None:
    for kwargs in (
        {"every_seconds": None, "cron_expr": None, "at": None},
        {"every_seconds": 60, "cron_expr": "* * * * *", "at": None},
        {"every_seconds": None, "cron_expr": "* * * * *", "at": "2026-09-01T00:00:00Z"},
    ):
        with pytest.raises(InvalidScheduleError):
            canonicalize_schedule(
                **kwargs,
                timezone=None,
                user_timezone="UTC",
                now=NOW,
            )


def test_timezone_is_not_allowed_with_every_schedule() -> None:
    with pytest.raises(InvalidScheduleError):
        canonicalize_schedule(
            every_seconds=60,
            cron_expr=None,
            at=None,
            timezone="UTC",
            user_timezone="UTC",
            now=NOW,
        )


def test_cron_expression_is_canonicalized_and_uses_user_timezone() -> None:
    schedule = canonicalize_schedule(
        every_seconds=None,
        cron_expr="  0   9  * *  mon-fri ",
        at=None,
        timezone=None,
        user_timezone="Asia/Shanghai",
        now=datetime(2026, 8, 31, 0, 0, tzinfo=UTC),
    )

    assert schedule == ScheduleSpec(
        kind="cron",
        value="0 9 * * mon-fri",
        timezone="Asia/Shanghai",
        next_fire_at=datetime(2026, 8, 31, 1, 0, tzinfo=UTC),
    )


@pytest.mark.parametrize(
    "kwargs",
    [
        {"cron_expr": "0 9 * * *", "at": None},
        {"cron_expr": None, "at": "2026-09-01T09:00:00"},
    ],
)
def test_explicit_empty_timezone_is_not_treated_as_omitted(
    kwargs: dict[str, str | None],
) -> None:
    with pytest.raises(InvalidTimezoneError):
        canonicalize_schedule(
            every_seconds=None,
            timezone="",
            user_timezone="Asia/Shanghai",
            now=NOW,
            **kwargs,
        )


@pytest.mark.parametrize(
    "expression",
    [
        "@daily",
        "0 0 0 * * *",
        "0 0 * *",
        "0 0 L * *",
        "0 0 1W * *",
        "0 0 * * MON#2",
        "H 0 * * *",
        "R 0 * * *",
        "0 0 32 * *",
        "0 0 31 2 *",
    ],
)
def test_cron_expression_rejects_extensions_ranges_and_no_future_dates(
    expression: str,
) -> None:
    with pytest.raises(InvalidScheduleError):
        canonicalize_schedule(
            every_seconds=None,
            cron_expr=expression,
            at=None,
            timezone="UTC",
            user_timezone="UTC",
            now=NOW,
        )


def test_vixie_day_of_month_or_day_of_week_semantics() -> None:
    schedule = canonicalize_schedule(
        every_seconds=None,
        cron_expr="0 9 1 * MON",
        at=None,
        timezone="UTC",
        user_timezone="UTC",
        now=datetime(2026, 9, 1, 9, 0, tzinfo=UTC),
    )

    # The next Monday wins even though it is not the first day of the month.
    assert schedule.next_fire_at == datetime(2026, 9, 7, 9, 0, tzinfo=UTC)


def test_naive_one_shot_uses_effective_timezone() -> None:
    schedule = canonicalize_schedule(
        every_seconds=None,
        cron_expr=None,
        at="2026-09-01T09:00:00",
        timezone=None,
        user_timezone="Asia/Shanghai",
        now=NOW,
    )

    assert schedule == ScheduleSpec(
        kind="at",
        value="2026-09-01T01:00:00Z",
        timezone="Asia/Shanghai",
        next_fire_at=datetime(2026, 9, 1, 1, 0, tzinfo=UTC),
    )


def test_aware_one_shot_without_timezone_uses_utc_projection() -> None:
    schedule = canonicalize_schedule(
        every_seconds=None,
        cron_expr=None,
        at="2026-09-01T09:00:00+08:00",
        timezone=None,
        user_timezone="America/New_York",
        now=NOW,
    )

    assert schedule.timezone == "UTC"
    assert schedule.value == "2026-09-01T01:00:00Z"
    assert schedule.next_fire_at == datetime(2026, 9, 1, 1, 0, tzinfo=UTC)


def test_aware_one_shot_timezone_offset_must_match() -> None:
    with pytest.raises(InvalidScheduleError):
        canonicalize_schedule(
            every_seconds=None,
            cron_expr=None,
            at="2026-09-01T09:00:00+07:00",
            timezone="Asia/Shanghai",
            user_timezone="UTC",
            now=NOW,
        )


@pytest.mark.parametrize(
    "local_time",
    ["2026-03-08T02:30:00", "2026-11-01T01:30:00"],
)
def test_naive_one_shot_rejects_nonexistent_and_ambiguous_local_time(
    local_time: str,
) -> None:
    with pytest.raises(InvalidScheduleError):
        canonicalize_schedule(
            every_seconds=None,
            cron_expr=None,
            at=local_time,
            timezone="America/New_York",
            user_timezone="UTC",
            now=datetime(2026, 1, 1, tzinfo=UTC),
        )


def test_one_shot_must_be_strictly_future() -> None:
    with pytest.raises(InvalidScheduleError):
        canonicalize_schedule(
            every_seconds=None,
            cron_expr=None,
            at=NOW.isoformat(),
            timezone=None,
            user_timezone="UTC",
            now=NOW,
        )


def test_cron_skips_nonexistent_spring_forward_occurrence() -> None:
    schedule = canonicalize_schedule(
        every_seconds=None,
        cron_expr="30 2 * * *",
        at=None,
        timezone="America/New_York",
        user_timezone="UTC",
        now=datetime(2026, 3, 8, 6, 0, tzinfo=UTC),
    )

    assert schedule.next_fire_at == datetime(2026, 3, 9, 6, 30, tzinfo=UTC)


def test_cron_fall_back_occurrence_uses_earlier_utc_instant_once() -> None:
    before_fold = canonicalize_schedule(
        every_seconds=None,
        cron_expr="30 1 * * *",
        at=None,
        timezone="America/New_York",
        user_timezone="UTC",
        now=datetime(2026, 11, 1, 4, 0, tzinfo=UTC),
    )
    after_first_fold = advance_recurring(
        before_fold,
        scheduled_at=before_fold.next_fire_at,
        now=datetime(2026, 11, 1, 5, 31, tzinfo=UTC),
    )

    assert before_fold.next_fire_at == datetime(2026, 11, 1, 5, 30, tzinfo=UTC)
    assert after_first_fold == datetime(2026, 11, 2, 6, 30, tzinfo=UTC)


def test_latest_due_cron_finds_earlier_fold_after_wall_clock_repeats() -> None:
    schedule = ScheduleSpec(
        kind="cron",
        value="30 1 * * *",
        timezone="America/New_York",
        next_fire_at=datetime(2026, 10, 31, 5, 30, tzinfo=UTC),
    )

    assert latest_due_occurrence(
        schedule,
        scheduled_at=schedule.next_fire_at,
        now=datetime(2026, 11, 1, 6, 15, tzinfo=UTC),
    ) == datetime(2026, 11, 1, 5, 30, tzinfo=UTC)


def test_latest_due_cron_uses_last_earlier_fold_match() -> None:
    schedule = ScheduleSpec(
        kind="cron",
        value="* 1 * * *",
        timezone="America/New_York",
        next_fire_at=datetime(2026, 10, 31, 5, 0, tzinfo=UTC),
    )

    assert latest_due_occurrence(
        schedule,
        scheduled_at=schedule.next_fire_at,
        now=datetime(2026, 11, 1, 6, 15, tzinfo=UTC),
    ) == datetime(2026, 11, 1, 5, 59, tzinfo=UTC)


def test_every_advancement_uses_saved_boundary_without_drift() -> None:
    schedule = ScheduleSpec(
        kind="every",
        value="60",
        timezone=None,
        next_fire_at=datetime(2026, 9, 1, 0, 1, tzinfo=UTC),
    )

    assert advance_recurring(
        schedule,
        scheduled_at=schedule.next_fire_at,
        now=datetime(2026, 9, 1, 0, 3, 1, tzinfo=UTC),
    ) == datetime(2026, 9, 1, 0, 4, tzinfo=UTC)


def test_at_schedule_cannot_be_advanced() -> None:
    schedule = ScheduleSpec(
        kind="at",
        value="2026-09-01T00:01:00Z",
        timezone="UTC",
        next_fire_at=datetime(2026, 9, 1, 0, 1, tzinfo=UTC),
    )

    with pytest.raises(InvalidScheduleError):
        advance_recurring(schedule, scheduled_at=schedule.next_fire_at, now=NOW)
