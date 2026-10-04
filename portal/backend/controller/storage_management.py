"""System storage controls; independent from V2's read-only primary rooms."""
from typing import Any, Callable

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, ConfigDict, StrictInt, StrictStr

from core.settings import get_settings
from core.storage_mounts import StorageMountError
from ..service.storage_management import StorageConflict, storage_management_service

router = APIRouter()


class StrictBody(BaseModel):
    model_config = ConfigDict(extra="forbid")


class RegisterTarget(StrictBody):
    target_id: StrictStr


class PlanRequest(StrictBody):
    policy: dict[str, Any]
    base_revision: StrictInt
    request_id: StrictStr


class ApplyRequest(StrictBody):
    policy_hash: StrictStr


def _mutation_origin(request: Request) -> None:
    # Preserve the existing local/private operator boundary. Browser writes
    # from unrelated sites are rejected, not merely hidden by CORS.
    origin = request.headers.get("origin")
    if origin and origin not in get_settings().backend.allowed_origins:
        raise HTTPException(403, "storage_origin_not_allowed")


def _call(action: Callable[[], Any]) -> Any:
    try:
        return action()
    except StorageConflict as exc:
        raise HTTPException(409, str(exc)) from exc
    except KeyError as exc:
        raise HTTPException(404, str(exc.args[0])) from exc
    except StorageMountError as exc:
        raise HTTPException(409, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc


@router.get("")
def storage_status() -> dict[str, Any]:
    return _call(storage_management_service.snapshot)


@router.post("/targets")
def register_target(body: RegisterTarget, request: Request) -> dict[str, Any]:
    _mutation_origin(request)
    return _call(lambda: storage_management_service.register(body.target_id))


@router.post("/plans")
def plan_change(body: PlanRequest, request: Request) -> dict[str, Any]:
    _mutation_origin(request)
    return _call(lambda: storage_management_service.plan(**body.model_dump()))


@router.get("/plans/{plan_id}")
def read_plan(plan_id: str) -> dict[str, Any]:
    return _call(lambda: storage_management_service.get_plan(plan_id))


@router.post("/plans/{plan_id}/apply")
def apply_plan(plan_id: str, body: ApplyRequest, request: Request) -> dict[str, Any]:
    _mutation_origin(request)
    return _call(lambda: storage_management_service.queue_plan(plan_id, policy_hash=body.policy_hash))
