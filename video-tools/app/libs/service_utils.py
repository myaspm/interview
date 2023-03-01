from datetime import datetime
from enum import Enum
from uuid import uuid4
from dataclasses import dataclass, field

import orjson as json

from conf.config import (
    SERVICE_NAME,
    WORKER_ID,
    STREAM_STATUS,
    GROUP_ID,
    WORKER_HEARTHBEAT_TTL,
)
from conf.log import log

from libs.redis_utils import get_connection, write_to_stream, STATUS


class WorkerStatus(Enum):
    IDLE = 0
    WORKING = 1


@dataclass
class WorkerSchema:
    status: WorkerStatus
    message_id: str = field(default=None)
    created_at: datetime = field(default=datetime.now())

    def to_json(self):
        return json.dumps(self)


def update_heartbeat(
    worker_id=WORKER_ID, service_name=SERVICE_NAME, conn=None
):

    log_msg = "Updating heartbeat"
    log.debug(log_msg)
    if not conn:
        conn = get_connection()

    key = f"service_{service_name}_heartbeat_{worker_id}"
    value = str(datetime.now())

    try:
        conn.set(key, value, ex=WORKER_HEARTHBEAT_TTL)
        log_msg = f"Heartbeat updated: {value}"
        log.debug(log_msg)
    except Exception as e:
        log_msg = f"Heartbeat update failed: {value} [Reason: {e}]"
        log.error(log_msg)


def update_status(
    worker_schema, worker_id=WORKER_ID, service_name=SERVICE_NAME, conn=None
):

    log_msg = f"Updating Status: {worker_schema.to_json()}"
    log.debug(log_msg)
    if not conn:
        conn = get_connection()

    key = f"service_{service_name}_status_{worker_id}"
    value = worker_schema.to_json()

    try:
        # update latest worker schema
        conn.set(key, value)
        log_msg = f"Status updated: {value}"
        log.debug(log_msg)

        # add latest worker schema to stream
        if STREAM_STATUS:
            key = f"{worker_id}_{str(uuid4())}"
            message = {
                "key": key,
                "val": value,
                "conn": conn,
                "stream": STATUS,
            }
            write_to_stream(**message)

    except Exception as e:
        log_msg = f"Status update failed: {value} [Reason: {e}]"
        log.error(log_msg)

    update_heartbeat(worker_id, service_name, conn)


def set_idle(worker_id=WORKER_ID, service_name=SERVICE_NAME, conn=None):
    s = {"status": WorkerStatus.IDLE}
    ws = WorkerSchema(**s)
    update_status(ws, worker_id, service_name, conn)


def set_working(
    id=None, worker_id=WORKER_ID, service_name=SERVICE_NAME, conn=None
):
    if id:
        s = {"status": WorkerStatus.WORKING, "message_id": id}
        ws = WorkerSchema(**s)
        update_status(ws, worker_id, service_name, conn)
    else:
        set_idle()


def register_service(
    worker_id=WORKER_ID, service_name=SERVICE_NAME, conn=None
):

    log.debug("Self Registering")
    if not conn:
        conn = get_connection()

    key = f"service_{service_name}_registration_{worker_id}_group_{GROUP_ID}"
    value = str(datetime.now())

    try:
        conn.set(key, value)
        log_msg = f"Registered: {value}"
        log.info(log_msg)
    except Exception as e:
        log_msg = f"Registration failed: {value} [Reason: {e}]"
        log.error(log_msg)

    set_idle(worker_id, service_name, conn)
