# logging_utils.py
import logging
import json
import requests
from datetime import datetime

class ExcludeLoggerFilter(logging.Filter):
    def __init__(self, excluded_names):
        self.excluded_names = excluded_names

    def filter(self, record):
        return not any(record.name.startswith(name) for name in self.excluded_names)

class LokiHandler(logging.Handler):
    def __init__(self, url: str, labels: dict = None, timeout: float = 2.0):
        super().__init__()
        self.url = url.rstrip("/") + "/loki/api/v1/push"
        self.labels = labels or {}
        self.timeout = timeout

        self.internal_logger = logging.getLogger("loki.internal")
        self.internal_logger.setLevel(logging.WARNING)
        self.internal_logger.propagate = False

    def emit(self, record):
        if record.name.startswith("urllib3") or record.name.startswith("requests"):
            return  # Avoid recursion

        try:
            line = self.format(record)
            ts = str(int(record.created * 1e9))
            payload = {
                "streams": [
                    {
                        "stream": self.labels,
                        "values": [[ts, line]]
                    }
                ]
            }

            resp = requests.post(self.url, json=payload, timeout=self.timeout)

            if resp.status_code >= 300:
                self.internal_logger.warning(f"Loki response: {resp.status_code} - {resp.text}")

        except Exception as e:
            self.internal_logger.error(f"[LokiHandler ERROR] {e}", exc_info=True)

