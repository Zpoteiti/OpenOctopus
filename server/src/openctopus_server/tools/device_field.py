from typing import Any

DEVICE_FIELD_NAME = "openoctopus_device"
DEVICE_FIELD_MARKER = "x-openoctopus-device"


def openoctopus_device_field(
    description: str,
    *,
    sites: list[str] | tuple[str, ...] = ("server",),
) -> dict[str, Any]:
    return {
        "type": "string",
        "enum": list(sites),
        "description": description,
        DEVICE_FIELD_MARKER: True,
    }
