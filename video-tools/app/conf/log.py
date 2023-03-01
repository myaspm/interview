import logging.config
from os import environ
from conf.config import WORKER_ID, SERVICE_NAME

l_format = (
    f"%(asctime)s - [%(levelname)s] [Service: {SERVICE_NAME}] "
    f"[Worker: {WORKER_ID}]: [%(funcName)s]: %(message)s"
)
log_level = environ.get("LOGLEVEL", default="INFO")

LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": True,
    "formatters": {
        "standard": {
            "format": l_format,
        },
    },
    "handlers": {
        "default": {
            "level": log_level,
            "formatter": "standard",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",  # Default is stderr
        },
        "file": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "INFO",
            "formatter": "standard",
            "filename": "service.log",
            "mode": "a",
            "maxBytes": 10485760,
            "backupCount": 5,
        },
        "hattusa": {
            "level": "WARNING",
            "formatter": "standard",
            "class": "conf.log_utils.ProgressConsoleHandler",
            "stream": "ext://os.devnull",
        },
    },
    "loggers": {
        "": {  # root logger
            "handlers": ["default", "hattusa", "file"],
            "level": "DEBUG",
            "propagate": False,
        }
    },
}

logging.config.dictConfig(LOGGING_CONFIG)
log = logging.getLogger(__name__)
