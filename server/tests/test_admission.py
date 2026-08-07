from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio

from openctopus_server.admission import (
    AdmissionTimeoutError,
    KeyedAdmission,
    KeyedDirectionalAdmission,
)


@pytest_asyncio.fixture(autouse=True)
async def _no_database_cleanup() -> AsyncIterator[None]:
    """Admission unit tests do not need PostgreSQL."""
    yield


async def test_per_user_waiter_does_not_take_a_global_slot() -> None:
    admission = KeyedAdmission(global_limit=2, per_key_limit=1, timeout_seconds=1)
    first_entered = asyncio.Event()
    release_first = asyncio.Event()
    second_entered = asyncio.Event()
    other_entered = asyncio.Event()

    async def hold_first() -> None:
        async with admission.slot("user-a"):
            first_entered.set()
            await release_first.wait()

    async def wait_same_user() -> None:
        async with admission.slot("user-a"):
            second_entered.set()

    async def hold_other_user() -> None:
        async with admission.slot("user-b"):
            other_entered.set()

    first = asyncio.create_task(hold_first())
    await first_entered.wait()
    same_user = asyncio.create_task(wait_same_user())
    await asyncio.sleep(0)
    other = asyncio.create_task(hold_other_user())

    await asyncio.wait_for(other_entered.wait(), timeout=0.5)
    assert not second_entered.is_set()

    release_first.set()
    await asyncio.gather(first, same_user, other)
    assert admission.entry_count == 0


async def test_timeout_releases_partial_acquisition_and_evicts_key() -> None:
    admission = KeyedAdmission(global_limit=1, per_key_limit=1, timeout_seconds=0.01)

    async with admission.slot("holder"):
        with pytest.raises(AdmissionTimeoutError):
            async with admission.slot("waiting"):
                raise AssertionError("timed-out admission entered")

        assert admission.entry_count == 1

    async with admission.slot("waiting"):
        pass
    assert admission.entry_count == 0


async def test_cancelled_waiter_releases_keyed_lease() -> None:
    admission = KeyedAdmission(global_limit=1, per_key_limit=1, timeout_seconds=10)

    async with admission.slot("holder"):
        waiter = asyncio.create_task(_enter_once(admission, "waiting"))
        await asyncio.sleep(0)
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter

        assert admission.entry_count == 1

    assert admission.entry_count == 0


async def test_directional_admission_shares_user_limit_but_not_global_pools() -> None:
    admission = KeyedDirectionalAdmission(
        direction_limits={"upload": 1, "download": 1},
        per_key_limit=1,
        timeout_seconds=1,
    )
    release_upload = asyncio.Event()
    upload_entered = asyncio.Event()
    same_user_download_entered = asyncio.Event()
    other_user_download_entered = asyncio.Event()

    async def upload() -> None:
        async with admission.slot("user-a", "upload"):
            upload_entered.set()
            await release_upload.wait()

    async def download(user: str, entered: asyncio.Event) -> None:
        async with admission.slot(user, "download"):
            entered.set()

    upload_task = asyncio.create_task(upload())
    await upload_entered.wait()
    same_user = asyncio.create_task(download("user-a", same_user_download_entered))
    other_user = asyncio.create_task(download("user-b", other_user_download_entered))

    await asyncio.wait_for(other_user_download_entered.wait(), timeout=0.5)
    assert not same_user_download_entered.is_set()

    release_upload.set()
    await asyncio.gather(upload_task, same_user, other_user)
    assert admission.entry_count == 0


async def test_directional_lease_can_be_transferred_and_closed_idempotently() -> None:
    admission = KeyedDirectionalAdmission(
        direction_limits={"download": 1},
        per_key_limit=1,
        timeout_seconds=0.01,
    )
    lease = await admission.acquire("user-a", "download")

    with pytest.raises(AdmissionTimeoutError):
        await admission.acquire("user-b", "download")

    await lease.aclose()
    await lease.aclose()
    replacement = await admission.acquire("user-b", "download")
    await replacement.aclose()

    assert admission.entry_count == 0


async def _enter_once(admission: KeyedAdmission, key: str) -> None:
    async with admission.slot(key):
        return
