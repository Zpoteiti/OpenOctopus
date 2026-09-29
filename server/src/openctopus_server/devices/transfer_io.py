"""Fenced transfer control-frame writes and transport failure normalization."""

from __future__ import annotations

from collections.abc import Callable

from .transfer_types import (
    TransferDisconnectedError,
    TransferRoute,
    TransferTransport,
)


async def send_transfer_text(
    transport: TransferTransport,
    handle: object,
    payload: str,
    *,
    route: TransferRoute | None,
    on_issued: Callable[[], None] | None = None,
) -> bool:
    try:
        if route is None:
            if on_issued is None:
                result = await transport.send_text(handle, payload)
            else:
                result = await transport.send_text(
                    handle,
                    payload,
                    on_issued=on_issued,
                )
        else:
            if on_issued is None:
                result = await transport.send_text(
                    handle,
                    payload,
                    expected_device_name=route.device_name,
                    expected_config_epoch=route.config_epoch,
                )
            else:
                result = await transport.send_text(
                    handle,
                    payload,
                    expected_device_name=route.device_name,
                    expected_config_epoch=route.config_epoch,
                    on_issued=on_issued,
                )
    except TransferDisconnectedError:
        raise
    except Exception as exc:
        raise TransferDisconnectedError("device transfer outcome is unknown") from exc
    return result is not False
