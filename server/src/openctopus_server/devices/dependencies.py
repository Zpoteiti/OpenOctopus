from functools import lru_cache

from openctopus_server.config import get_settings
from openctopus_server.devices.registry import DeviceRegistry


@lru_cache
def get_device_registry() -> DeviceRegistry:
    settings = get_settings()
    return DeviceRegistry(
        pending_calls_max=settings.device_pending_calls_max,
        pending_calls_max_per_user=settings.device_pending_calls_max_per_user,
        pending_bytes_max=settings.device_pending_bytes_max,
        pending_bytes_max_per_user=settings.device_pending_bytes_max_per_user,
        transfer_max_concurrency=settings.device_transfer_max_concurrency,
        transfer_max_concurrency_per_user=settings.device_transfer_max_concurrency_per_user,
        transfer_queue_timeout_seconds=settings.device_transfer_queue_timeout_seconds,
        transfer_idle_timeout_seconds=settings.device_transfer_idle_timeout_seconds,
    )
