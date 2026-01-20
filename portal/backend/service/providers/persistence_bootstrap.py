"""Configure provider persistence wiring for service-layer runtime."""

from data_providers.config.runtime import runtime_config_from_env
from data_providers.services import configure_provider_persistence as wire_provider_persistence

from .persistence import DataPersistenceService


def configure_provider_persistence() -> None:
    def _build_persistence():
        config = runtime_config_from_env()
        return DataPersistenceService(config.persistence)

    wire_provider_persistence(_build_persistence)


configure_provider_persistence()
