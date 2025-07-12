import logging
from .logging_utils import LokiHandler, ExcludeLoggerFilter

LOG_FMT = "%(asctime)s %(levelname)-5s %(filename)s:%(lineno)d | %(message)s"
logging.basicConfig(level=logging.DEBUG, format=LOG_FMT)

LOKI_URL = "http://localhost:3100"
LOKI_LABELS = {"app": "quant_trad", "env": "dev"}

loki_handler = LokiHandler(url=LOKI_URL, labels=LOKI_LABELS, timeout=1.0)
loki_handler.setLevel(logging.DEBUG)
loki_handler.setFormatter(logging.Formatter(LOG_FMT))
loki_handler.addFilter(ExcludeLoggerFilter(["urllib3", "requests", "loki.internal", "font_manager"]))

root_logger = logging.getLogger()
root_logger.addHandler(loki_handler)

logger = logging.getLogger(__name__)

# logging.basicConfig(
#     level=logging.DEBUG,
#     format="%(asctime)s - %(levelname)s - %(filename)s:%(funcName)s:%(lineno)d - %(message)s"
# )

# logger = logging.getLogger(__name__)