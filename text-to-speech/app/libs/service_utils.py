from datetime import datetime
from enum import Enum
import orjson as json

from dataclasses import dataclass, field

from config.config import (
    SERVICE_NAME,
    WORKER_ID,
)

from log.log import log

from .redis_utils import get_connection


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
    log.info(f"[Worker: {WORKER_ID}] {log_msg}")
    if not conn:
        conn = get_connection()

    key = f"service-{service_name}-heartbeat-{worker_id}"
    value = str(datetime.now())

    try:
        conn.set(key, value)
        log_msg = f"Heartbeat updated: {value}"
        log.info(f"[Worker: {WORKER_ID}] {log_msg}")
    except Exception as e:
        log_msg = f"Heartbeat update failed: {value} [Reason: {e}]"
        log.error(f"[Worker: {WORKER_ID}] {log_msg}")


def update_status(
    worker_schema, worker_id=WORKER_ID, service_name=SERVICE_NAME, conn=None
):

    log_msg = f"Updating Status: {worker_schema.to_json()}"
    log.info(f"[Worker: {WORKER_ID}] {log_msg}")
    if not conn:
        conn = get_connection()

    key = f"service-{service_name}-status-{worker_id}"
    value = worker_schema.to_json()

    try:
        conn.set(key, value)
        log_msg = f"Status updated: {value}"
        log.info(f"[Worker: {WORKER_ID}] {log_msg}")
    except Exception as e:
        log_msg = f"Status update failed: {value} [Reason: {e}]"
        log.error(f"[Worker: {WORKER_ID}] {log_msg}")

    update_heartbeat(worker_id, service_name, conn)


def set_idle(worker_id=WORKER_ID, service_name=SERVICE_NAME, conn=None):
    s = {"status": WorkerStatus.IDLE}
    ws = WorkerSchema(**s)
    update_status(ws, worker_id, service_name, conn)


def set_working(id, worker_id=WORKER_ID, service_name=SERVICE_NAME, conn=None):
    if id:
        s = {"status": WorkerStatus.WORKING, "message_id": id}
        ws = WorkerSchema(**s)
        update_status(ws, worker_id, service_name, conn)
    else:
        set_idle()


def register_service(
    worker_id=WORKER_ID, service_name=SERVICE_NAME, conn=None
):

    log_msg = "Self Registering"
    log.info(f"[Worker: {WORKER_ID}] {log_msg}")
    if not conn:
        conn = get_connection()

    key = f"service-{service_name}-registration-{worker_id}"
    value = str(datetime.now())

    try:
        conn.set(key, value)
        log_msg = f"Registered: {value}"
        log.info(f"[Worker: {WORKER_ID}] {log_msg}")
    except Exception as e:
        log_msg = f"Registration failed: {value} [Reason: {e}]"
        log.error(f"[Worker: {WORKER_ID}] {log_msg}")

    set_idle(worker_id, service_name, conn)
