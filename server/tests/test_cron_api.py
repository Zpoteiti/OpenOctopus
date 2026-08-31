from unittest.mock import Mock


async def test_cron_rest_crud_and_pagination(user_client, test_app) -> None:
    scheduler = Mock()
    test_app.state.cron_scheduler = scheduler

    created = await user_client.post(
        "/api/cron",
        json={
            "name": "weekday report",
            "message": "Prepare the weekday report.",
            "cron_expr": "0 9 * * 1-5",
            "tz": "Asia/Shanghai",
        },
    )
    assert created.status_code == 201
    payload = created.json()
    assert payload["schedule"] == {
        "type": "cron",
        "cron_expr": "0 9 * * 1-5",
        "tz": "Asia/Shanghai",
    }
    assert payload["session_id"] is None

    listed = await user_client.get("/api/cron", params={"limit": 1, "offset": 0})
    assert listed.status_code == 200
    assert listed.json()["items"][0]["name"] == "weekday report"
    assert "message" not in listed.json()["items"][0]

    fetched = await user_client.get(f"/api/cron/{payload['id']}")
    assert fetched.status_code == 200
    assert fetched.json()["message"] == "Prepare the weekday report."

    patched = await user_client.patch(
        f"/api/cron/{payload['id']}",
        json={"name": "new name", "every_seconds": 3600},
    )
    assert patched.status_code == 200
    assert patched.json()["name"] == "new name"
    assert patched.json()["schedule"] == {"type": "every", "every_seconds": 3600}

    deleted = await user_client.delete(f"/api/cron/{payload['id']}")
    assert deleted.status_code == 204
    missing = await user_client.get(f"/api/cron/{payload['id']}")
    assert missing.status_code == 404
    assert missing.json()["code"] == "cron_job_not_found"
    assert scheduler.wake.call_count == 3


async def test_cron_rest_is_owner_scoped(async_client) -> None:
    await async_client.post(
        "/api/auth/register",
        json={"email": "one@example.com", "password": "testpassword", "name": "One"},
    )
    await async_client.post(
        "/api/auth/login",
        json={"email": "one@example.com", "password": "testpassword"},
    )
    created = await async_client.post(
        "/api/cron",
        json={"message": "owner only", "every_seconds": 60},
    )
    job_id = created.json()["id"]

    await async_client.post(
        "/api/auth/register",
        json={"email": "two@example.com", "password": "testpassword", "name": "Two"},
    )
    await async_client.post(
        "/api/auth/login",
        json={"email": "two@example.com", "password": "testpassword"},
    )

    assert (await async_client.get(f"/api/cron/{job_id}")).status_code == 404
    assert (await async_client.patch(f"/api/cron/{job_id}", json={"name": "x"})).status_code == 404
    assert (await async_client.delete(f"/api/cron/{job_id}")).status_code == 404


async def test_cron_rest_rejects_invalid_schedule_with_stable_error(user_client) -> None:
    responses = (
        await user_client.post(
            "/api/cron",
            json={"message": "bad", "every_seconds": True},
        ),
        await user_client.post(
            "/api/cron",
            json={"message": "bad", "cron_expr": "@daily"},
        ),
        await user_client.post(
            "/api/cron",
            json={"message": "bad", "every_seconds": 60, "extra": 1},
        ),
    )
    assert [(response.status_code, response.json()["code"]) for response in responses] == [
        (400, "cron_invalid_schedule"),
        (400, "cron_invalid_schedule"),
        (400, "cron_invalid_schedule"),
    ]


async def test_cron_rest_rejects_invalid_timezone_with_stable_error(user_client) -> None:
    response = await user_client.post(
        "/api/cron",
        json={"message": "bad", "cron_expr": "* * * * *", "tz": "CST"},
    )

    assert response.status_code == 400
    assert response.json()["code"] == "timezone_invalid"
