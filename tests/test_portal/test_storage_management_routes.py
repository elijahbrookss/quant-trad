from fastapi import FastAPI
from fastapi.testclient import TestClient

from portal.backend.controller import storage_management as controller
from portal.backend.service.storage_management import StorageConflict


def client():
    app = FastAPI()
    app.include_router(controller.router, prefix="/api/storage")
    return TestClient(app)


def test_ui_cannot_submit_arbitrary_host_paths(monkeypatch):
    def unexpected(*args, **kwargs):
        raise AssertionError("registration must not be called")
    monkeypatch.setattr(controller.storage_management_service, "register", unexpected)
    result = client().post("/api/storage/targets", json={"target_id": "hdd", "root": "/etc"})
    assert result.status_code == 422


def test_foreign_origin_cannot_mutate_storage(monkeypatch):
    def unexpected(*args, **kwargs):
        raise AssertionError("registration must not be called")
    monkeypatch.setattr(controller.storage_management_service, "register", unexpected)
    result = client().post("/api/storage/targets", json={"target_id": "hdd"},
                           headers={"Origin": "https://unrelated.invalid"})
    assert result.status_code == 403


def test_stale_plan_is_a_conflict_not_success(monkeypatch):
    def stale(*args, **kwargs):
        raise StorageConflict("storage_policy_changed")
    monkeypatch.setattr(controller.storage_management_service, "queue_plan", stale)
    result = client().post("/api/storage/plans/example/apply", json={"policy_hash": "a" * 64})
    assert result.status_code == 409
    assert result.json()["detail"] == "storage_policy_changed"


def test_revision_does_not_coerce_boolean(monkeypatch):
    def unexpected(*args, **kwargs):
        raise AssertionError("planning must not be called")
    monkeypatch.setattr(controller.storage_management_service, "plan", unexpected)
    result = client().post("/api/storage/plans", json={"policy": {}, "base_revision": True, "request_id": "one"})
    assert result.status_code == 422
