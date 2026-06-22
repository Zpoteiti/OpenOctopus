from datetime import UTC, datetime, timedelta
from uuid import UUID

import jwt

from openctopus_server.config import get_settings
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import AuthError

_ALGORITHM = "HS256"
_EXP_DAYS = 30


def create_jwt(user_id: UUID) -> str:
    settings = get_settings()
    now = datetime.now(UTC)
    payload = {
        "sub": str(user_id),
        "exp": now + timedelta(days=_EXP_DAYS),
    }
    return jwt.encode(payload, settings.jwt_secret, algorithm=_ALGORITHM)


def verify_jwt(token: str) -> UUID:
    settings = get_settings()
    try:
        payload = jwt.decode(token, settings.jwt_secret, algorithms=[_ALGORITHM])
    except jwt.PyJWTError:
        raise AuthError(ErrorCode.AUTH_UNAUTHORIZED, "Invalid or expired token")
    sub = payload.get("sub")
    if not sub:
        raise AuthError(ErrorCode.AUTH_UNAUTHORIZED, "Invalid token payload")
    return UUID(sub)
