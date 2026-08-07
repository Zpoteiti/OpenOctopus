from __future__ import annotations

import sys
from importlib import metadata
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from check_content_conversion_memory import (  # noqa: E402
    ACCEPTANCE_CONCURRENCY,
    ACCEPTANCE_MEMORY_MB,
    ACCEPTANCE_REPEAT,
    Arguments,
    EvaluationInput,
    Fixture,
    MiB,
    ProcessRecord,
    ProtocolExercise,
    RunnerError,
    _validate_acceptance_configuration,
    discover_fixtures,
    evaluate_gates,
    parse_proc_status,
    resolved_package_versions,
)


def _protocol_exercise(
    *,
    rounds_completed: int = ACCEPTANCE_REPEAT,
    file_descriptors_after: int = 10,
) -> ProtocolExercise:
    return ProtocolExercise(
        rounds_requested=ACCEPTANCE_REPEAT,
        rounds_completed=rounds_completed,
        success_count=rounds_completed,
        malformed_count=rounds_completed,
        timeout_count=rounds_completed,
        cancellation_count=rounds_completed,
        permit_acquisitions=rounds_completed * 4,
        admission_entries_after=0,
        leaked_child_pids=(),
        file_descriptors_before=10,
        file_descriptors_after=file_descriptors_after,
        failure=None,
    )


def _record(
    label: str,
    *,
    ok: bool = True,
    code: str = "",
    high_water_bytes: int = 100 * MiB,
    wall_seconds: float = 1.0,
) -> ProcessRecord:
    return ProcessRecord(
        label=label,
        fixture=label,
        ok=ok,
        code=code,
        output_chars=10,
        wall_seconds=wall_seconds,
        pid=123,
        exit_code=0,
        ru_maxrss_bytes=high_water_bytes,
        proc_high_water_bytes=high_water_bytes,
        max_sampled_rss_bytes=high_water_bytes,
    )


def test_parse_proc_status_converts_linux_kib_values_to_bytes() -> None:
    parsed = parse_proc_status("Name:\tpython\nVmRSS:\t 123 kB\nVmHWM:\t456 kB\nThreads:\t1\n")

    assert parsed == {"rss_bytes": 123 * 1024, "high_water_bytes": 456 * 1024}


def test_discover_fixtures_requires_every_supported_format(tmp_path: Path) -> None:
    for suffix in ("pdf", "docx", "xlsx", "pptx"):
        (tmp_path / f"sample.{suffix}").write_bytes(b"fixture")

    with pytest.raises(RunnerError, match=r"missing required fixture format: html"):
        discover_fixtures(tmp_path)


def test_discover_fixtures_is_deterministic_and_builds_format_requests(tmp_path: Path) -> None:
    for name in ("z.pdf", "a.docx", "b.xlsx", "c.pptx", "page.html"):
        (tmp_path / name).write_bytes(name.encode())

    fixtures = discover_fixtures(tmp_path)

    assert [fixture.relative_path for fixture in fixtures] == [
        "a.docx",
        "b.xlsx",
        "c.pptx",
        "page.html",
        "z.pdf",
    ]
    html = next(fixture for fixture in fixtures if fixture.conversion_format == "html")
    assert html.request(memory_mb=1024, timeout_seconds=20) == {
        "operation": "html",
        "data": b"page.html",
        "charset": "utf-8",
        "base_url": "https://example.invalid/fixture/",
        "mode": "markdown",
        "max_chars": 50_000,
        "memory_mb": 1024,
        "timeout_seconds": 20.0,
    }


def test_versioned_acceptance_corpus_contains_every_supported_format() -> None:
    fixtures = discover_fixtures(Path(__file__).parent / "fixtures" / "documents")

    assert {fixture.conversion_format for fixture in fixtures} == {
        "pdf",
        "docx",
        "xlsx",
        "pptx",
        "html",
    }


def test_evaluate_gates_accepts_values_exactly_on_all_hard_limits() -> None:
    baseline = 200 * MiB
    memory_limit = 1024 * MiB
    normal = (_record("docx", high_water_bytes=memory_limit),)
    concurrent = (
        _record("concurrent-1", high_water_bytes=memory_limit),
        _record("concurrent-2", high_water_bytes=memory_limit),
    )
    evaluation = EvaluationInput(
        normal_records=normal,
        over_limit_record=_record(
            "over-limit",
            ok=False,
            code="tool_content_conversion_resource_exceeded",
        ),
        recovery_record=_record("recovery"),
        concurrent_records=concurrent,
        concurrent_peak_rss_bytes=baseline + 2 * memory_limit + 256 * MiB,
        concurrent_synchronized_participants=2,
        baseline_parent_rss_bytes=baseline,
        final_parent_rss_bytes=baseline + 64 * MiB,
        memory_limit_bytes=memory_limit,
        timeout_seconds=20,
        owned_resources_released=True,
        protocol_exercise=_protocol_exercise(),
    )

    gates = evaluate_gates(evaluation)

    assert all(bool(gate["passed"]) for gate in gates.values())


def test_evaluate_gates_rejects_parent_or_child_one_byte_over_limit() -> None:
    baseline = 200 * MiB
    memory_limit = 1024 * MiB
    evaluation = EvaluationInput(
        normal_records=(_record("docx", high_water_bytes=memory_limit + 1),),
        over_limit_record=_record(
            "over-limit",
            ok=False,
            code="tool_content_conversion_resource_exceeded",
        ),
        recovery_record=_record("recovery"),
        concurrent_records=(_record("one"), _record("two")),
        concurrent_peak_rss_bytes=baseline,
        concurrent_synchronized_participants=2,
        baseline_parent_rss_bytes=baseline,
        final_parent_rss_bytes=baseline + 64 * MiB + 1,
        memory_limit_bytes=memory_limit,
        timeout_seconds=20,
        owned_resources_released=True,
        protocol_exercise=_protocol_exercise(),
    )

    gates = evaluate_gates(evaluation)

    assert gates["normal_child_memory"]["passed"] is False
    assert gates["parent_rss_growth"]["passed"] is False


def test_evaluate_gates_checks_concurrent_child_memory_and_start_barrier() -> None:
    memory_limit = 1024 * MiB
    evaluation = EvaluationInput(
        normal_records=(_record("docx"),),
        over_limit_record=_record(
            "over-limit",
            ok=False,
            code="tool_content_conversion_resource_exceeded",
        ),
        recovery_record=_record("recovery"),
        concurrent_records=(
            _record("one"),
            _record("two", high_water_bytes=memory_limit + 1),
        ),
        concurrent_peak_rss_bytes=0,
        concurrent_synchronized_participants=1,
        baseline_parent_rss_bytes=200 * MiB,
        final_parent_rss_bytes=200 * MiB,
        memory_limit_bytes=memory_limit,
        timeout_seconds=20,
        owned_resources_released=True,
        protocol_exercise=_protocol_exercise(),
    )

    gates = evaluate_gates(evaluation)

    assert gates["normal_child_memory"]["passed"] is False
    assert gates["concurrent_start_barrier"]["passed"] is False


def test_evaluate_gates_rejects_incomplete_parent_protocol_exercise() -> None:
    evaluation = EvaluationInput(
        normal_records=(_record("docx"),),
        over_limit_record=_record(
            "over-limit",
            ok=False,
            code="tool_content_conversion_resource_exceeded",
        ),
        recovery_record=_record("recovery"),
        concurrent_records=(_record("one"), _record("two")),
        concurrent_peak_rss_bytes=0,
        concurrent_synchronized_participants=2,
        baseline_parent_rss_bytes=200 * MiB,
        final_parent_rss_bytes=200 * MiB,
        memory_limit_bytes=1024 * MiB,
        timeout_seconds=20,
        owned_resources_released=True,
        protocol_exercise=_protocol_exercise(rounds_completed=19),
    )

    gates = evaluate_gates(evaluation)

    assert gates["mixed_parent_protocol"]["passed"] is False


def test_evaluate_gates_rejects_a_parent_protocol_file_descriptor_leak() -> None:
    evaluation = EvaluationInput(
        normal_records=(_record("docx"),),
        over_limit_record=_record(
            "over-limit",
            ok=False,
            code="tool_content_conversion_resource_exceeded",
        ),
        recovery_record=_record("recovery"),
        concurrent_records=(_record("one"), _record("two")),
        concurrent_peak_rss_bytes=0,
        concurrent_synchronized_participants=2,
        baseline_parent_rss_bytes=200 * MiB,
        final_parent_rss_bytes=200 * MiB,
        memory_limit_bytes=1024 * MiB,
        timeout_seconds=20,
        owned_resources_released=True,
        protocol_exercise=_protocol_exercise(file_descriptors_after=11),
    )

    gates = evaluate_gates(evaluation)

    assert gates["mixed_parent_protocol"]["passed"] is False


def test_fixture_rejects_an_oversized_input_before_spawning(tmp_path: Path) -> None:
    path = tmp_path / "huge.pdf"
    path.write_bytes(b"")
    fixture = Fixture(
        path=path,
        relative_path="huge.pdf",
        conversion_format="pdf",
        data=b"x" * (8 * MiB + 1),
    )

    with pytest.raises(RunnerError, match="8 MiB"):
        fixture.validate_size()


def test_dependency_preflight_lists_all_missing_packages(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def version(distribution: str) -> str:
        if distribution in {"markitdown", "pypdf"}:
            raise metadata.PackageNotFoundError(distribution)
        return "1.0"

    monkeypatch.setattr(metadata, "version", version)

    with pytest.raises(
        RunnerError,
        match="missing required conversion dependencies: markitdown, pypdf",
    ):
        resolved_package_versions()


def test_acceptance_configuration_is_fixed_and_matches_server_settings(
    tmp_path: Path,
) -> None:
    arguments = Arguments(
        fixtures=tmp_path,
        memory_mb=ACCEPTANCE_MEMORY_MB,
        concurrency=ACCEPTANCE_CONCURRENCY,
        repeat=ACCEPTANCE_REPEAT,
        output=tmp_path / "evidence.json",
    )

    _validate_acceptance_configuration(
        arguments,
        configured_memory_mb=ACCEPTANCE_MEMORY_MB,
        configured_concurrency=ACCEPTANCE_CONCURRENCY,
    )


def test_acceptance_configuration_rejects_non_acceptance_repeat(tmp_path: Path) -> None:
    arguments = Arguments(
        fixtures=tmp_path,
        memory_mb=ACCEPTANCE_MEMORY_MB,
        concurrency=ACCEPTANCE_CONCURRENCY,
        repeat=ACCEPTANCE_REPEAT - 1,
        output=tmp_path / "evidence.json",
    )

    with pytest.raises(RunnerError, match=rf"--repeat must be {ACCEPTANCE_REPEAT}"):
        _validate_acceptance_configuration(
            arguments,
            configured_memory_mb=ACCEPTANCE_MEMORY_MB,
            configured_concurrency=ACCEPTANCE_CONCURRENCY,
        )
