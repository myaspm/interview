"""

This module is standard microservice entry point

"""

from subscriptions import init_subscriptions
from subjects import request_subject as rs
from libs.redis_utils import read_from_stream, get_connection, JOBS

from redis.exceptions import ResponseError
from config import REDIS_JOBS_CONSUMER_GROUP
from log import log


def read():
    # log.info("Querying streams")
    for msg in read_from_stream():
        rs.on_next(msg)
    read()


def run():
    init_subscriptions(rs)
    log.info("Observables are initiated")
    try:
        read()
    except ResponseError as re:
        log.warning("Stream channel and/or consumer group error")
        log.warning(re)
        r = get_connection()
        r.xgroup_create(JOBS, REDIS_JOBS_CONSUMER_GROUP, mkstream=True)
        print(re)
    except Exception as e:
        log.error(f"Error occured: {e}")

    finally:
        read()


if __name__ == "__main__":
    run()
