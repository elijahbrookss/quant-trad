"""Explicit bot-runtime persistence gateway over domain-owned repositories."""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional, Protocol

from ..storage.repos.bots import upsert_bot
from ..storage.repos.lifecycle import (
    get_bot_run_lifecycle,
    get_latest_bot_run_lifecycle,
    list_latest_bot_run_lifecycles,
    rebuild_bot_run_lifecycle_summary,
    record_bot_run_lifecycle_checkpoint,
)
from ..storage.repos.report_materializations import (
    get_report_materialization_status,
    list_report_materialization_statuses,
)
from ..storage.repos.run_leases import (
    acquire_bot_run_lease,
    get_bot_run_lease,
    list_bot_run_leases_by_run_ids,
    release_bot_run_lease,
)
from ..storage.repos.runs import (
    get_bot_run,
    list_bot_runs,
    list_bot_runs_by_ids,
    list_bot_runs_page,
    list_latest_bot_runs_by_bot_ids,
    upsert_bot_run,
)
from ..storage.repos.runtime_events import (
    get_latest_bot_runtime_run_id,
    list_botlens_run_evidence,
)


class BotStorageGateway(Protocol):
    """Persistence operations required by bot runtime orchestration."""

    def upsert_bot(self, payload: Mapping[str, Any]) -> None: ...

    def upsert_bot_run(self, payload: Mapping[str, Any]) -> Dict[str, Any]: ...

    def get_bot_run(self, run_id: str) -> Optional[Dict[str, Any]]: ...

    def list_bot_runs_by_ids(
        self,
        run_ids: List[str],
    ) -> Dict[str, Dict[str, Any]]: ...

    def list_latest_bot_runs_by_bot_ids(
        self,
        bot_ids: List[str],
    ) -> Dict[str, Dict[str, Any]]: ...

    def get_report_materialization_status(self, run_id: str) -> Dict[str, Any]: ...

    def list_report_materialization_statuses(
        self,
        run_ids: List[str],
    ) -> Dict[str, Dict[str, Any]]: ...

    def get_latest_bot_runtime_run_id(self, bot_id: str) -> Optional[str]: ...

    def list_botlens_run_evidence(
        self, run_ids: List[str]
    ) -> Dict[str, Dict[str, Any]]: ...

    def get_bot_run_lifecycle(
        self,
        run_id: str,
    ) -> Optional[Mapping[str, Any]]: ...

    def get_bot_run_lease(
        self,
        run_id: str,
    ) -> Optional[Mapping[str, Any]]: ...

    def list_bot_run_leases_by_run_ids(
        self,
        run_ids: List[str],
    ) -> Dict[str, Dict[str, Any]]: ...

    def acquire_bot_run_lease(
        self,
        *,
        bot_id: str,
        run_id: str,
        runner_id: str,
        lease_token: str,
        ttl_seconds: float | int | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> Dict[str, Any]: ...

    def release_bot_run_lease(
        self,
        *,
        bot_id: str,
        run_id: str,
        runner_id: str | None = None,
        lease_token: str | None = None,
        status: str = "released",
        metadata: Mapping[str, Any] | None = None,
    ) -> Optional[Dict[str, Any]]: ...

    def get_latest_bot_run_lifecycle(
        self,
        bot_id: str,
    ) -> Optional[Mapping[str, Any]]: ...

    def list_latest_bot_run_lifecycles(
        self,
        bot_ids: List[str],
        *,
        run_ids_by_bot: Mapping[str, str] | None = None,
    ) -> Dict[str, Dict[str, Any]]: ...

    def record_bot_run_lifecycle_checkpoint(
        self,
        payload: Mapping[str, Any],
    ) -> Dict[str, Any]: ...

    def rebuild_bot_run_lifecycle_summary(
        self,
        run_id: str,
    ) -> Dict[str, Any]: ...

    def list_bot_runs(
        self,
        *,
        bot_id: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]: ...

    def list_bot_runs_page(
        self,
        *,
        limit: int = 100,
        before_sort_at: Optional[str] = None,
        before_run_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]: ...


class RepositoryBotStorageGateway:
    """Bot gateway whose methods delegate to one named repository owner."""

    def upsert_bot(self, payload: Mapping[str, Any]) -> None:
        upsert_bot(dict(payload))

    def upsert_bot_run(self, payload: Mapping[str, Any]) -> Dict[str, Any]:
        return upsert_bot_run(dict(payload))

    def get_bot_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        return get_bot_run(str(run_id))

    def list_bot_runs_by_ids(
        self,
        run_ids: List[str],
    ) -> Dict[str, Dict[str, Any]]:
        return list_bot_runs_by_ids([str(run_id) for run_id in run_ids])

    def list_latest_bot_runs_by_bot_ids(
        self,
        bot_ids: List[str],
    ) -> Dict[str, Dict[str, Any]]:
        return list_latest_bot_runs_by_bot_ids(
            [str(bot_id) for bot_id in bot_ids]
        )

    def get_report_materialization_status(self, run_id: str) -> Dict[str, Any]:
        return get_report_materialization_status(str(run_id))

    def list_report_materialization_statuses(
        self,
        run_ids: List[str],
    ) -> Dict[str, Dict[str, Any]]:
        return list_report_materialization_statuses(
            [str(run_id) for run_id in run_ids]
        )

    def get_latest_bot_runtime_run_id(self, bot_id: str) -> Optional[str]:
        return get_latest_bot_runtime_run_id(str(bot_id))

    def list_botlens_run_evidence(
        self, run_ids: List[str]
    ) -> Dict[str, Dict[str, Any]]:
        return list_botlens_run_evidence([str(run_id) for run_id in run_ids])

    def get_bot_run_lifecycle(
        self,
        run_id: str,
    ) -> Optional[Mapping[str, Any]]:
        return get_bot_run_lifecycle(str(run_id))

    def get_bot_run_lease(
        self,
        run_id: str,
    ) -> Optional[Mapping[str, Any]]:
        return get_bot_run_lease(str(run_id))

    def list_bot_run_leases_by_run_ids(
        self,
        run_ids: List[str],
    ) -> Dict[str, Dict[str, Any]]:
        return list_bot_run_leases_by_run_ids(
            [str(run_id) for run_id in run_ids]
        )

    def acquire_bot_run_lease(
        self,
        *,
        bot_id: str,
        run_id: str,
        runner_id: str,
        lease_token: str,
        ttl_seconds: float | int | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> Dict[str, Any]:
        return acquire_bot_run_lease(
            bot_id=str(bot_id),
            run_id=str(run_id),
            runner_id=str(runner_id),
            lease_token=str(lease_token),
            ttl_seconds=ttl_seconds,
            metadata=metadata,
        )

    def release_bot_run_lease(
        self,
        *,
        bot_id: str,
        run_id: str,
        runner_id: str | None = None,
        lease_token: str | None = None,
        status: str = "released",
        metadata: Mapping[str, Any] | None = None,
    ) -> Optional[Dict[str, Any]]:
        return release_bot_run_lease(
            bot_id=str(bot_id),
            run_id=str(run_id),
            runner_id=runner_id,
            lease_token=lease_token,
            status=status,
            metadata=metadata,
        )

    def get_latest_bot_run_lifecycle(
        self,
        bot_id: str,
    ) -> Optional[Mapping[str, Any]]:
        return get_latest_bot_run_lifecycle(str(bot_id))

    def list_latest_bot_run_lifecycles(
        self,
        bot_ids: List[str],
        *,
        run_ids_by_bot: Mapping[str, str] | None = None,
    ) -> Dict[str, Dict[str, Any]]:
        return list_latest_bot_run_lifecycles(
            [str(bot_id) for bot_id in bot_ids],
            run_ids_by_bot={
                str(key): str(value)
                for key, value in dict(run_ids_by_bot or {}).items()
            },
        )

    def record_bot_run_lifecycle_checkpoint(
        self,
        payload: Mapping[str, Any],
    ) -> Dict[str, Any]:
        return record_bot_run_lifecycle_checkpoint(dict(payload))

    def rebuild_bot_run_lifecycle_summary(
        self,
        run_id: str,
    ) -> Dict[str, Any]:
        return rebuild_bot_run_lifecycle_summary(str(run_id))

    def list_bot_runs(
        self,
        *,
        bot_id: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        rows = list_bot_runs(bot_id=bot_id)
        if limit and int(limit) > 0:
            return list(rows)[: int(limit)]
        return list(rows)

    def list_bot_runs_page(
        self,
        *,
        limit: int = 100,
        before_sort_at: Optional[str] = None,
        before_run_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        return list_bot_runs_page(
            limit=limit,
            before_sort_at=before_sort_at,
            before_run_id=before_run_id,
        )


def build_bot_storage_gateway() -> BotStorageGateway:
    """Build the canonical bot orchestration persistence gateway."""

    return RepositoryBotStorageGateway()


__all__ = [
    "BotStorageGateway",
    "RepositoryBotStorageGateway",
    "build_bot_storage_gateway",
]
