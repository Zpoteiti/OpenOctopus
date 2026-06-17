"""Shared pytest fixtures and configuration."""

import pytest

from openctopus_server.config import get_settings


@pytest.fixture(autouse=True)
def _clear_settings_cache():
    """Ensure the settings singleton cache is cleared around every test."""
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()
