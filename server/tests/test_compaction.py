import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.db.models import SystemConfig
from openctopus_server.errors.exceptions import ChatError
from openctopus_server.provider.config import load_provider_config


async def test_provider_config_loads_and_validates_compaction_threshold(pg_engine) -> None:
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        db.add_all(
            [
                SystemConfig(key="llm_endpoint", value="http://fake.test"),
                SystemConfig(key="llm_api_key", value="fake-key"),
                SystemConfig(key="llm_model", value="fake-model"),
                SystemConfig(key="llm_max_context_tokens", value=128_000),
                SystemConfig(key="llm_compaction_threshold_tokens", value=16_000),
            ]
        )
        await db.commit()
        config = await load_provider_config(db)
        assert config.compaction_threshold_tokens == 16_000

        threshold = await db.get(SystemConfig, "llm_compaction_threshold_tokens")
        assert threshold is not None
        threshold.value = 4000
        await db.commit()
        with pytest.raises(ChatError, match="llm_compaction_threshold_tokens is invalid"):
            await load_provider_config(db)

        threshold.value = 128_000
        await db.commit()
        with pytest.raises(ChatError, match="llm_compaction_threshold_tokens is invalid"):
            await load_provider_config(db)

        threshold.value = 16_000
        context = await db.get(SystemConfig, "llm_max_context_tokens")
        assert context is not None
        await db.delete(context)
        await db.commit()
        with pytest.raises(ChatError, match="llm_compaction_threshold_tokens is invalid"):
            await load_provider_config(db)
