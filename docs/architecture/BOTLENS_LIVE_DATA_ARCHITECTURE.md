# BotLens Live Data Architecture (Superseded)

This document has been superseded by:
- `docs/architecture/NEW_BOTLENS_ARCHITECTURE.md`

BotLens now uses a strict two-input model:
- REST window/history for bootstrap and paging
- WS run/series live-tail deltas only

Legacy bot-level envelope/replay descriptions in earlier versions are no longer valid.
