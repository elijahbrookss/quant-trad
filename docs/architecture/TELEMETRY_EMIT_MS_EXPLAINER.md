# Telemetry Emit Timing (Updated)

For current BotLens architecture, see:
- `docs/architecture/NEW_BOTLENS_ARCHITECTURE.md`

Key update:
- BotLens no longer uses `/lens/bootstrap` + `/api/bots/ws/{bot_id}` legacy flow.
- It now uses run/series REST window/history and run/series live-tail WS.
