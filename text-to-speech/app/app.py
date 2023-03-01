"""

This module is standard microservice entry point

"""

from subscriptions import init_subscriptions
from subjects import request_subject as rs
from libs.redis_utils import read_from_stream, get_connection, JOBS
from libs.service_utils import register_service, set_idle

from redis.exceptions import ResponseError
from config.config import REDIS_JOBS_CONSUMER_GROUP
from log.log import log


def read():
    # log.info("Querying streams")
    for msg in read_from_stream():
        rs.on_next(msg)

    set_idle()
    read()


def init():
    try:
        register_service()
        read()
    except ResponseError as re:
        log.warning("Stream channel and/or consumer group error")
        log.warning(re)
        r = get_connection()
        r.xgroup_create(JOBS, REDIS_JOBS_CONSUMER_GROUP, mkstream=True)


def run():
    init_subscriptions(rs)
    log.info("Observables are initiated")
    try:
        init()
    except Exception as e:
        log.error(f"Error occured: {e}")
    finally:
        run()


if __name__ == "__main__":
    run()
