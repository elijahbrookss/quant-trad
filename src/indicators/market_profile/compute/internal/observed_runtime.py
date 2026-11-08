"""Walk-forward daily revision replacement and observed coverage publication."""
from copy import deepcopy
from dataclasses import dataclass

import pandas as pd

from market_data.frozen import semantic_hash

from .runtime_profiles import IncrementalRuntimeProfileResolver, profile_identity
from ..models import Profile


@dataclass(frozen=True)
class ObservedProfile(Profile):
    runtime_revision_key: str = ""


class ObservedSessionResolver:
    def __init__(self, *, profiles_payload, profile_params, symbol=None):
        self._events = sorted(deepcopy(profiles_payload), key=lambda entry: (
            entry["known_at"], entry["session_key"], entry["session_revision"]))
        self._params = dict(profile_params)
        self._symbol = symbol
        self._next = 0
        self._sessions = {}
        self._epoch = None
        self._effective = []
        self._summary = {"known_profiles": 0, "merged_profiles": 0}
        self.quality = None

    def resolve(self, *, current_epoch):
        if self._epoch is not None and current_epoch < self._epoch:
            raise RuntimeError("market_profile_runtime_epoch_invalid: observed session clock moved backwards")
        self._epoch = current_epoch
        changed = False
        while self._next < len(self._events) and self._events[self._next]["known_at"] <= current_epoch:
            entry = self._events[self._next]
            self._sessions[entry["session_key"]] = entry
            self._next += 1
            changed = True
        if changed:
            # A late row replaces its day; it must never count as another day.
            # Reuse the established merge owner on the current causal daily set.
            current = [self._sessions[key] for key in sorted(self._sessions)]
            merger = IncrementalRuntimeProfileResolver.from_known_sessions(
                profiles_payload=current, profile_params=self._params, symbol=self._symbol,
                current_epoch=current_epoch)
            self._effective, self._summary = merger.resolve(current_epoch=current_epoch)
            self.quality = None
            for index, profile in enumerate(self._effective):
                members = [entry for entry in current
                           if entry["start"] >= int(profile.start.timestamp())
                           and entry["end"] <= int(profile.end.timestamp())]
                if len(members) != profile.session_count:
                    raise RuntimeError("market_profile_source_contract_invalid: observed merge membership mismatch")
                revision_key = semantic_hash([entry["quality"]["source_lineage_hash"] for entry in members])
                profile = ObservedProfile(start=profile.start, end=profile.end,
                    value_area=profile.value_area, session_count=profile.session_count,
                    tpo_histogram=profile.tpo_histogram, precision=profile.precision,
                    runtime_revision_key=revision_key)
                self._effective[index] = profile
                if index == len(self._effective)-1:
                    self.quality = self._quality(profile, members)
        return list(self._effective), dict(self._summary)

    @staticmethod
    def _quality(profile, members):
        sessions = [deepcopy(entry["quality"]) for entry in members]
        observed = sum(session["observed_bars"] for session in sessions)
        dates = [pd.Timestamp(session["session"]) for session in sessions]
        day = pd.Timedelta(days=1)
        expected = 48 * ((dates[-1]-dates[0]).days+1)
        absent = [{"start": (left+day).isoformat(), "end_exclusive": right.isoformat(),
                   "missing_days": (right-left).days-1}
                  for left, right in zip(dates, dates[1:]) if right-left > day]
        pocs = [entry["POC"] for entry in members]
        deletions = [(sum(pocs)-value)/(len(pocs)-1) for value in pocs] if len(pocs) > 1 else []
        return {
            "state_key": "complete_observed_slots" if observed == expected else "observed_incomplete",
            "fields": {
                "active_profile_key": profile_identity(profile), "sessions": sessions,
                "unobserved_session_ranges": absent,
                "observed_bars": observed, "expected_bars": expected,
                "missing_bars": expected-observed,
                "availability_inferred": any(session["availability_inferred"] for session in sessions),
                "availability_methods": sorted({method for session in sessions for method in session["availability_methods"]}),
                "predictive_qualification": False,
                "missing_bar_price_impact": "unknown",
                "leave_one_session_out_poc_min": min(deletions) if deletions else None,
                "leave_one_session_out_poc_max": max(deletions) if deletions else None,
                "max_leave_one_session_out_poc_shift": max(abs(value-profile.poc) for value in deletions) if deletions else None,
            },
        }
