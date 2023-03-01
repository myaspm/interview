import logging
from uuid import uuid4


import redis
from conf.config import (
    LOG_REDIS_HOST,
    LOG_REDIS_PORT,
    LOG_REDIS_DB,
    LOG_REDIS_PW,
    LOG_STREAM,
    LOG_REDIS,
)


LOG_CONN = None


def get_connection(**kwargs):
    global LOG_CONN
    if LOG_CONN is None:
        host = kwargs.get("host", LOG_REDIS_HOST)
        port = kwargs.get("port", LOG_REDIS_PORT)
        db = kwargs.get("db", LOG_REDIS_DB)
        password = kwargs.get("password", LOG_REDIS_PW)
        pool = redis.ConnectionPool(
            host=host, port=port, db=db, password=password
        )
        LOG_CONN = redis.Redis(connection_pool=pool)

    return LOG_CONN


class ProgressConsoleHandler(logging.StreamHandler):
    def emit(self, record):
        if LOG_REDIS:
            try:
                msg = self.format(record)
                conn = get_connection()
                key = str(uuid4())
                val = msg
                stream = LOG_STREAM
                conn.xadd(stream, {key: val})

            except (KeyboardInterrupt, SystemExit):
                raise
            except:
                self.handleError(record)
