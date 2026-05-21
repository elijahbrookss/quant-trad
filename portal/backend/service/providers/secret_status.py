"""Secret availability helpers for provider/venue gating."""

from __future__ import annotations

from typing import Dict, List, Optional

from data_providers.registry import get_provider_config, get_venue_config
from data_providers.services.credential_store import has_credentials, list_credentials


def _credential_refs(provider_id: Optional[str], venue_id: Optional[str]) -> List[dict]:
    try:
        return [item.to_public_dict() for item in list_credentials(provider_id=provider_id, venue_id=venue_id)]
    except Exception:
        return []


def required_keys(provider_id: Optional[str], venue_id: Optional[str]) -> List[str]:
    """Return the secrets required for a provider/venue combo using registry metadata."""

    venue_cfg = get_venue_config(venue_id)
    if venue_cfg and venue_cfg.required_secrets:
        return venue_cfg.required_secrets

    provider_cfg = get_provider_config(provider_id)
    if provider_cfg and provider_cfg.required_secrets:
        return provider_cfg.required_secrets

    return []


def optional_keys(provider_id: Optional[str], venue_id: Optional[str]) -> List[str]:
    """Return optional credential keys accepted by a provider/venue combo."""

    venue_cfg = get_venue_config(venue_id)
    if venue_cfg and venue_cfg.optional_secrets:
        return venue_cfg.optional_secrets

    provider_cfg = get_provider_config(provider_id)
    if provider_cfg and provider_cfg.optional_secrets:
        return provider_cfg.optional_secrets

    return []


def resolve_status(
    provider_id: Optional[str],
    venue_id: Optional[str],
    *,
    credential_ref: Optional[str] = None,
    environment: Optional[str] = None,
) -> Dict[str, object]:
    keys = required_keys(provider_id, venue_id)
    if not keys:
        refs = _credential_refs(provider_id, venue_id)
        return {"state": "available", "missing": [], "required": [], "credentials": refs}

    try:
        refs = _credential_refs(provider_id, venue_id)
        if has_credentials(provider_id, venue_id, keys, credential_ref=credential_ref, environment=environment):
            return {"state": "available", "missing": [], "required": keys, "credentials": refs}
    except Exception as exc:  # pragma: no cover - configuration issues
        message = str(exc)
        state = "error"
        if "Failed to decrypt provider credentials" in message or "InvalidToken" in message:
            state = "invalid_credentials"
        return {
            "state": state,
            "missing": keys,
            "required": keys,
            "message": message,
            "action": "resave_credentials" if state == "invalid_credentials" else None,
        }

    return {"state": "missing_secrets", "missing": keys, "required": keys, "credentials": refs}


__all__ = ["optional_keys", "resolve_status", "required_keys"]
