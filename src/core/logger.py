import logging
import os
from dotenv import load_dotenv
from utils.logging_utils import LokiHandler, ExcludeLoggerFilter

# Only load .env if not running inside GitHub Actions
if not os.getenv("GITHUB_ACTIONS"):
    load_dotenv("secrets.env")
    load_dotenv(".env")

debug_mode = os.getenv("DEBUG", "false").lower() == "true"

LOG_FMT = "%(asctime)s %(levelname)-5s %(filename)s:%(lineno)d | %(message)s"
logging.basicConfig(level=logging.DEBUG if debug_mode else logging.INFO, format=LOG_FMT)

root_logger = logging.getLogger()

# Loki config
LOKI_URL = "http://localhost:3100"
LOKI_LABELS = {"app": "quant_trad", "env": os.getenv("ENV", "dev") }

try:
    loki_handler = LokiHandler(url=LOKI_URL, labels=LOKI_LABELS, timeout=1.0)
    loki_handler.setLevel(logging.DEBUG if debug_mode else logging.INFO)
    loki_handler.setFormatter(logging.Formatter(LOG_FMT))
    loki_handler.addFilter(ExcludeLoggerFilter(["urllib3", "requests", "loki.internal"]))
    root_logger.addHandler(loki_handler)
    root_logger.debug("Loki logging handler successfully configured.")
except Exception as e:
    root_logger.warning(f"Loki logging not configured (skipping): {e}")

# Reduce noise from 3rd party libs
logging.getLogger("matplotlib.font_manager").setLevel(logging.WARNING)
logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)
