import logging
from datetime import datetime, timezone

from pythonjsonlogger.json import JsonFormatter

from tasks.task3.config import settings

logger = logging.getLogger("task3")
handler = logging.StreamHandler()


class CustomJsonFormatter(JsonFormatter):
    """Форматер в виде JSON"""

    def __init__(self, *args, **kwargs):
        kwargs['json_ensure_ascii'] = False
        super().__init__(*args, **kwargs)

    def add_fields(self, log_record, record, message_dict):
        super(CustomJsonFormatter, self).add_fields(log_record, record, message_dict)
        if not log_record.get('timestamp'):
            now = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            log_record['timestamp'] = now
        if log_record.get('level'):
            log_record['level'] = log_record['level'].upper()
        else:
            log_record['level'] = record.levelname


formatter = CustomJsonFormatter('%(timestamp)s %(level)s %(name)s %(message)s %(module)s %(funcName)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(settings.LOG_LEVEL)