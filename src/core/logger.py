import logging
from utils.logging_utils import RuntimeContextFormatter, runtime_log_context_from_env
from core.settings import ensure_env_loaded, get_settings

ensure_env_loaded()
_SETTINGS = get_settings()

log_level = _SETTINGS.logging.level

LOG_FMT = "%(asctime)s %(levelname)-5s %(filename)s:%(lineno)d | %(message)s"
logging.basicConfig(level=log_level, format=LOG_FMT)
_FORMATTER = RuntimeContextFormatter(LOG_FMT, context=runtime_log_context_from_env())

root_logger = logging.getLogger()
root_logger.setLevel(log_level)

for handler in root_logger.handlers:
    handler.setLevel(log_level)
    handler.setFormatter(_FORMATTER)

LOKI_URL = (_SETTINGS.logging.loki_url or "").strip()
if LOKI_URL:
    root_logger.warning(
        "direct_loki_logging_disabled | reason=promtail_stdout_primary | configured_loki_url=%s",
        LOKI_URL,
    )

# Reduce noise from 3rd party libs
logging.getLogger("matplotlib.font_manager").setLevel(logging.WARNING)
logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)
logging.getLogger("ccxt.base.exchange").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)
