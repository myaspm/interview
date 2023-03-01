"""

This module is standard microservice entry point

"""
from os import environ
from redis.exceptions import ResponseError
from commons.subscriptions import init_subscriptions
from commons.subjects import (
    request_subject as rs,
    event_subject as es,
    leader_subject as ls,
)
from commons.schedulers import start_scheduler
from libs.redis_utils import (
    read_from_stream,
    get_connection,
    JOBS,
    am_i_leader,
    CLUSTER,
)
from libs.service_utils import register_service, set_idle


from conf.config import REDIS_JOBS_CONSUMER_GROUP, HAS_SCHEDULER
from conf.log import log


def read():
    for msg in read_from_stream():
        rs.on_next(msg)

    set_idle()


def init():
    try:
        environ[
            "GOOGLE_APPLICATION_CREDENTIALS"
        ] = "service/libs/creovideo-ea8a00cde04c.json"
        register_service()
        running_scheduler = None
        while True:
            try:
                if HAS_SCHEDULER:
                    # check leader status.  This function also triggers other
                    # functions that controls the APScheduler state
                    am_i_leader()
                read()
            except Exception as e:
                log.warning("Stream channel and/or consumer group error")
                log.warning(e)
                r = get_connection()
                r.xgroup_create(JOBS, REDIS_JOBS_CONSUMER_GROUP, mkstream=True)

    except ResponseError as re:
        log.warning("Stream channel and/or consumer group error")
        log.warning(re)


def run():
    init_subscriptions(request_subject=rs, event_subject=es, leader_subject=ls)
    log.info("Observables are initiated")
    try:
        init()
    except Exception as exc:
        msg = f"Error occured: {exc}"
        log.error(msg)
        log.exception(exc)


if __name__ == "__main__":
    run()
