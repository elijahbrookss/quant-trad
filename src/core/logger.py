import logging
from utils.logging_utils import LokiHandler, ExcludeLoggerFilter
from core.settings import ensure_env_loaded, get_settings

ensure_env_loaded()
_SETTINGS = get_settings()

debug_mode = _SETTINGS.logging.debug
log_level = _SETTINGS.logging.level

LOG_FMT = "%(asctime)s %(levelname)-5s %(filename)s:%(lineno)d | %(message)s"
logging.basicConfig(level=log_level, format=LOG_FMT)

root_logger = logging.getLogger()
root_logger.setLevel(log_level)

for handler in root_logger.handlers:
    handler.setLevel(log_level)

# Loki config
LOKI_URL = (_SETTINGS.logging.loki_url or "").strip()
LOKI_LABELS = {"app": "quant_trad", "env": _SETTINGS.logging.env_name}

if LOKI_URL:
    try:
        loki_handler = LokiHandler(url=LOKI_URL, labels=LOKI_LABELS, timeout=1.0)
        loki_handler.setLevel(logging.DEBUG if debug_mode else logging.INFO)
        loki_handler.setFormatter(logging.Formatter(LOG_FMT))
        loki_handler.addFilter(ExcludeLoggerFilter(["urllib3", "requests", "loki.internal"]))
        root_logger.addHandler(loki_handler)
        root_logger.debug("Loki logging handler successfully configured.")
    except Exception as e:
        root_logger.warning(f"Loki logging not configured (skipping): {e}")
else:
    root_logger.debug("LOKI_URL not set; skipping Loki logging handler configuration.")

# Reduce noise from 3rd party libs
logging.getLogger("matplotlib.font_manager").setLevel(logging.WARNING)
logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)
logging.getLogger("ccxt.base.exchange").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)
