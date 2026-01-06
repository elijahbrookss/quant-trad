"""Configure provider persistence wiring for service-layer runtime."""

from data_providers.config.runtime import runtime_config_from_env
from data_providers.providers import factory as provider_factory

from .persistence import DataPersistenceService


def configure_provider_persistence() -> None:
    def _build_persistence():
        config = runtime_config_from_env()
        return DataPersistenceService(config.persistence)

    provider_factory.configure_persistence_factory(_build_persistence)


configure_provider_persistence()
