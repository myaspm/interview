import redis
from config.config import (
    REDIS_HOST,
    REDIS_PORT,
    REDIS_DB,
    REDIS_PW,
    SERVICE_NAME,
    REDIS_JOBS_CONSUMER_GROUP,
    READ_N,
    WORKER_ID,
)

from log.log import log

CONN = None

JOBS = f"{SERVICE_NAME}"
COMPLETED = f"{SERVICE_NAME}_completed"
PROCESSING = f"{SERVICE_NAME}_processing"
ERROR = f"{SERVICE_NAME}_error"


def get_connection(**kwargs):
    global CONN
    if CONN is None:
        log_msg = "No redis connection.  Initiating a new connection"
        log.info(f"[Worker: {WORKER_ID}] {log_msg}")
        host = kwargs.get("host", REDIS_HOST)
        port = kwargs.get("port", REDIS_PORT)
        db = kwargs.get("db", REDIS_DB)
        password = kwargs.get("password", REDIS_PW)
        pool = redis.ConnectionPool(
            host=host, port=port, db=db, password=password
        )
        CONN = redis.Redis(connection_pool=pool)
        log_msg = f"Redis Connected: {host}:{port}"
        log.info(f"[Worker: {WORKER_ID}] {log_msg}")

    return CONN


def set_message(msg, conn=None):
    if not conn:
        conn = get_connection()

    id = msg.id
    service = msg.service

    key = f"{service}-{id}"
    value = msg.to_json()

    conn.set(key, value)


def ack_message(**kwargs):
    conn = kwargs.get("conn", None)
    if not conn:
        log_msg = "No Redis Connection"
        log.info(f"[Worker: {WORKER_ID}] {log_msg}")
        conn = get_connection()

    id = kwargs.get("id", None)

    result = 0
    if id:
        log_msg = f"Acknowledging Message:{id}"
        log.info(f"[Worker: {WORKER_ID}] {log_msg}")
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
        block=6000,
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
        log.info(f"[Worker: {WORKER_ID}] {log_msg}")
    except Exception as e:
        log_msg = "Error at writing message to stream:" + str(e)
        log.info(f"[Worker: {WORKER_ID}] {log_msg}")
