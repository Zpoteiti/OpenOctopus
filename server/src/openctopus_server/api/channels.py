from __future__ import annotations

from typing import cast
from uuid import UUID

from fastapi import APIRouter, Depends, Request, Response
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.auth.dependencies import get_current_user
from openctopus_server.channels.types import ExternalChannel
from openctopus_server.db.models import User
from openctopus_server.db.session import get_db
from openctopus_server.dto.channel import (
    ChannelBotResponse,
    ChannelConfigPatchRequest,
    ChannelConfigResponse,
    ChannelLastErrorResponse,
    ChannelOwnerResponse,
    ChannelPairingResponse,
)
from openctopus_server.dto.error import ErrorResponse
from openctopus_server.services import channels
from openctopus_server.services.channels import (
    ChannelConfigPatch,
    ChannelConfigRuntime,
    ChannelCredentialsUnverifiedError,
    ChannelCredentialValidator,
    ChannelRuntimeSnapshot,
    ValidatedBotIdentity,
)

router = APIRouter(prefix="/api/channels", tags=["Channels"])


class _UnavailableCredentialValidator:
    async def validate_discord(self, bot_token: str) -> ValidatedBotIdentity:
        del bot_token
        raise ChannelCredentialsUnverifiedError

    async def validate_dingtalk(
        self,
        client_id: str,
        client_secret: str,
    ) -> ValidatedBotIdentity:
        del client_id, client_secret
        raise ChannelCredentialsUnverifiedError


class _UnavailableRuntime:
    def status(
        self,
        user_id: UUID,
        channel: ExternalChannel,
    ) -> ChannelRuntimeSnapshot | None:
        del user_id, channel
        return None

    async def apply(self, user_id: UUID, channel: ExternalChannel) -> None:
        del user_id, channel
        raise RuntimeError("channel runtime is unavailable")

    async def remove(self, user_id: UUID, channel: ExternalChannel) -> None:
        del user_id, channel


_UNAVAILABLE_VALIDATOR = _UnavailableCredentialValidator()
_UNAVAILABLE_RUNTIME = _UnavailableRuntime()


def get_channel_credential_validator(request: Request) -> ChannelCredentialValidator:
    validator = getattr(request.app.state, "channel_credential_validator", None)
    return cast(
        ChannelCredentialValidator,
        validator if validator is not None else _UNAVAILABLE_VALIDATOR,
    )


def get_channel_runtime(request: Request) -> ChannelConfigRuntime:
    runtime = getattr(request.app.state, "channel_runtime", None)
    return cast(
        ChannelConfigRuntime,
        runtime if runtime is not None else _UNAVAILABLE_RUNTIME,
    )


@router.get("", response_model=list[ChannelConfigResponse])
async def list_channels(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    runtime: ChannelConfigRuntime = Depends(get_channel_runtime),
) -> list[ChannelConfigResponse]:
    user_id = user.id
    views = await channels.list_configs(db, user_id=user_id, runtime=runtime)
    return [_response(view) for view in views]


@router.patch(
    "/{channel}",
    response_model=ChannelConfigResponse,
    responses={
        400: {"model": ErrorResponse},
        409: {"model": ErrorResponse},
        503: {"model": ErrorResponse},
    },
)
async def patch_channel(
    channel: str,
    body: ChannelConfigPatchRequest,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    validator: ChannelCredentialValidator = Depends(
        get_channel_credential_validator
    ),
    runtime: ChannelConfigRuntime = Depends(get_channel_runtime),
) -> ChannelConfigResponse:
    user_id = user.id
    view = await channels.patch_config(
        db,
        user_id=user_id,
        channel=channel,
        patch=ChannelConfigPatch(
            bot_token=body.bot_token,
            client_id=body.client_id,
            client_secret=body.client_secret,
            allow_list=(
                tuple(body.allow_list) if body.allow_list is not None else None
            ),
        ),
        validator=validator,
        runtime=runtime,
    )
    return _response(view)


@router.post(
    "/{channel}/pairing",
    response_model=ChannelConfigResponse,
    responses={
        400: {"model": ErrorResponse},
        404: {"model": ErrorResponse},
        409: {"model": ErrorResponse},
    },
)
async def rotate_pairing(
    channel: str,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    runtime: ChannelConfigRuntime = Depends(get_channel_runtime),
) -> ChannelConfigResponse:
    user_id = user.id
    view = await channels.rotate_pairing_code(
        db,
        user_id=user_id,
        channel=channel,
        runtime=runtime,
    )
    return _response(view)


@router.delete(
    "/{channel}",
    status_code=204,
    response_model=None,
    responses={400: {"model": ErrorResponse}},
)
async def delete_channel(
    channel: str,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    runtime: ChannelConfigRuntime = Depends(get_channel_runtime),
) -> Response:
    user_id = user.id
    await channels.delete_config(
        db,
        user_id=user_id,
        channel=channel,
        runtime=runtime,
    )
    return Response(status_code=204)


def _response(view: channels.ChannelConfigView) -> ChannelConfigResponse:
    pairing = (
        ChannelPairingResponse(
            expires_at=view.pairing_expires_at,
            code=view.pairing_code,
        )
        if view.pairing_expires_at is not None
        else None
    )
    last_error = (
        ChannelLastErrorResponse(
            code=view.last_error.code,
            message=view.last_error.message,
            at=view.last_error.at,
        )
        if view.last_error is not None
        else None
    )
    return ChannelConfigResponse(
        channel=view.channel,
        configured=view.configured,
        state=view.state,
        bot=(
            ChannelBotResponse(
                id=view.bot_id,
                name=view.bot_name,
                avatar_url=view.bot_avatar_url,
            )
            if view.bot_id is not None
            else None
        ),
        owner=(
            ChannelOwnerResponse(
                id=view.owner_id,
                dm_chat_id=view.owner_dm_chat_id,
            )
            if view.owner_id is not None and view.owner_dm_chat_id is not None
            else None
        ),
        allow_list=list(view.allow_list),
        credential_hint="Configured" if view.configured else None,
        pairing=pairing,
        last_error=last_error,
    )
