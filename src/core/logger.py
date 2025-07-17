import logging
from utils.logging_utils import LokiHandler, ExcludeLoggerFilter
from dotenv import load_dotenv
import os

load_dotenv("secrets.env")
load_dotenv(".env")

debug_mode = True if os.getenv("DEBUG", "false").lower() == "true" else False

LOG_FMT = "%(asctime)s %(levelname)-5s %(filename)s:%(lineno)d | %(message)s"
logging.basicConfig(level=logging.DEBUG, format=LOG_FMT)

LOKI_URL = "http://localhost:3100"
LOKI_LABELS = {"app": "quant_trad", "env": os.getenv("ENV", "dev") }

loki_handler = LokiHandler(url=LOKI_URL, labels=LOKI_LABELS, timeout=1.0)
loki_handler.setLevel(logging.DEBUG if debug_mode else logging.INFO)
loki_handler.setFormatter(logging.Formatter(LOG_FMT))
loki_handler.addFilter(ExcludeLoggerFilter(["urllib3", "requests", "loki.internal"]))

root_logger = logging.getLogger()
root_logger.addHandler(loki_handler)

logger = logging.getLogger(__name__)

logging.getLogger("matplotlib.font_manager").setLevel(logging.WARNING)
logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)