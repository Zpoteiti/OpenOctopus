from uuid import uuid4

import jwt
import pytest

from openctopus_server.auth.jwt import create_jwt, verify_jwt
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import AuthError


def test_create_and_verify_jwt_round_trip():
    user_id = uuid4()
    token = create_jwt(user_id)
    assert verify_jwt(token) == user_id


def test_verify_jwt_rejects_tampered_token():
    user_id = uuid4()
    token = create_jwt(user_id)
    tampered = token[:-5] + "AAAAA" if token[-5:] != "AAAAA" else token[:-5] + "BBBBB"
    with pytest.raises(AuthError) as exc_info:
        verify_jwt(tampered)
    assert exc_info.value.code == ErrorCode.AUTH_UNAUTHORIZED


def test_verify_jwt_rejects_garbage():
    with pytest.raises(AuthError) as exc_info:
        verify_jwt("not-a-jwt")
    assert exc_info.value.code == ErrorCode.AUTH_UNAUTHORIZED


def test_jwt_contains_sub_and_exp_only():
    user_id = uuid4()
    token = create_jwt(user_id)
    payload = jwt.decode(token, options={"verify_signature": False})
    assert payload["sub"] == str(user_id)
    assert "exp" in payload
    assert "is_admin" not in payload
