"""Shared pytest fixtures and configuration."""

import pytest

from openctopus_server.config import get_settings
from openctopus_server.db.engine import get_engine


@pytest.fixture(autouse=True)
def _clear_settings_and_engine_cache():
    """Ensure singleton caches are cleared around every test."""
    get_settings.cache_clear()
    get_engine.cache_clear()
    yield
    get_engine.cache_clear()
    get_settings.cache_clear()
