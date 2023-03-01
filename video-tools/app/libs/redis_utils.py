"""Helper functions for Redis """

import redis
from conf.config import (
    REDIS_HOST,
    REDIS_PORT,
    REDIS_DB,
    REDIS_PW,
    SERVICE_NAME,
    REDIS_JOBS_CONSUMER_GROUP,
    REDIS_JOB_POLL_BLOCK,
    READ_N,
    WORKER_ID,
)

from conf.log import log
from commons.subjects import leader_subject

# WARNING! Globale state variable.  Use with caution
CONN = None  # Redis connection global state

JOBS = f"stream_{SERVICE_NAME}_messages"
STATUS = f"stream_{SERVICE_NAME}_message_statuses"
CLUSTER = f"cluster_{SERVICE_NAME}_{REDIS_JOBS_CONSUMER_GROUP}"


def get_connection(**kwargs):
    """If global variable conn has a connection returns the conn variable
    If the conn is not set, creates a new redis connection and set the
    global variable conn and returns the new connection
    """
    global CONN
    if CONN is None:
        log_msg = "No redis connection.  Initiating a new connection"
        log.debug(log_msg)
        host = kwargs.get("host", REDIS_HOST)
        port = kwargs.get("port", REDIS_PORT)
        db = kwargs.get("db", REDIS_DB)
        password = kwargs.get("password", REDIS_PW)
        pool = redis.ConnectionPool(
            host=host, port=port, db=db, password=password
        )
        CONN = redis.Redis(connection_pool=pool)
        log_msg = f"Redis Connected: {host}:{port}"
        log.debug(log_msg)

    try:
        ping = CONN.ping()  # check if the connection is alive
        if not ping:
            raise Exception("Redis connection is dead. No ping reply")
    except Exception as exc:
        log_msg = "Redis connection is dead.  Initiating a new connection"
        log.warning(log_msg)
        CONN = None  # remove the dead redis object
        return get_connection(**kwargs)

    return CONN


def scan_redis_keys(pattern, conn=None):
    "Returns a list of all the keys matching a given pattern"
    if not conn:
        conn = get_connection()

    result = []
    cur, keys = conn.scan(cursor=0, match=pattern, count=2)
    result.extend(keys)
    while cur != 0:
        cur, keys = conn.scan(cursor=cur, match=pattern, count=2)
        result.extend(keys)

    result = [key.decode() for key in result]

    return result


def set_message(msg, conn=None):
    if not conn:
        conn = get_connection()

    id = msg.id
    service = msg.service

    key = f"message_{service}_id_{id}"
    value = msg.to_json()

    conn.set(key, value)


def ack_message(**kwargs):
    conn = kwargs.get("conn", None)
    if not conn:
        log_msg = "No Redis Connection"
        log.info(f"{log_msg}")
        conn = get_connection()

    id = kwargs.get("id", None)

    result = 0
    if id:
        log_msg = f"Acknowledging Message:{id}"
        log.info(f"{log_msg}")
        result = conn.xack(JOBS, REDIS_JOBS_CONSUMER_GROUP, id)

    return result


def read_from_stream(**kwargs):
    conn = kwargs.get("conn", None)
    if not conn:
        conn = get_connection()
    entries = conn.xreadgroup(
        REDIS_JOBS_CONSUMER_GROUP,
        WORKER_ID,
        {JOBS: ">"},
        count=READ_N,
        block=REDIS_JOB_POLL_BLOCK,
    )

    for stream_entries in entries:
        for message in stream_entries[1]:
            id = message[0].decode()
            key = list(message[1].keys())[0].decode()
            msg = list(message[1].values())[0].decode()
            yield (id, key, msg)


def write_to_stream(**kwargs):
    conn = kwargs.get("conn", None)
    if not conn:
        conn = get_connection()
    key = kwargs.get("key", None)
    val = kwargs.get("val", None)
    stream = kwargs.get("stream", None)
    try:
        conn.xadd(stream, {key: val})
        log_msg = "Message with ID {} is sent to {}".format(key, stream)
        log.debug(log_msg)
    except Exception as exc:
        log_msg = f"Error at writing message to stream: {exc}"
        log.error(log_msg)
        log.exception(exc)


def get_leader_lock(**kwargs):
    cluster_key = f"{CLUSTER}_master"

    conn = kwargs.get("conn", None)
    if not conn:
        conn = get_connection()

    try:
        log.debug(f"Trying to get leader lock for {CLUSTER}")
        master = conn.set(cluster_key, WORKER_ID, ex=60, nx=True)
        if master:
            msg = f"Reset the leader lock for {CLUSTER}"
            log.debug(msg)
            msg = f"New elect leader {CLUSTER}: {WORKER_ID}"
            log.info(msg)
            leader_subject.on_next(True)
            return WORKER_ID
        else:
            master_instance = conn.get(cluster_key).decode()
            msg = f"Lock for {CLUSTER} is set by {master_instance}"
            log.debug(msg)
            if master_instance != WORKER_ID:
                # another worker instance is leader
                leader_subject.on_next(False)
            else:
                # this worker is still the leader
                leader_subject.on_next(True)
            return master_instance

    except Exception as exc:
        msg = f"Cannot get leader lock for {CLUSTER} | reason: {exc}"
        log.error(msg)
        log.exception(exc)


def am_i_leader(**kwargs):
    return WORKER_ID == get_leader_lock(**kwargs)
