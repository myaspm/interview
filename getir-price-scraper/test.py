from uuid import uuid4
from datetime import datetime
from libs.models import (
    ParamsSchema,
    StatusSchema,
    MessageSchema,
    MessageStatus,
)

from config import SERVICE_NAME, REDIS_JOBS_CONSUMER_GROUP, WORKER_ID

from libs.redis_utils import get_connection, JOBS


def create_message():
    p = {
        "blacklisted_cats": ['Kategoriler', 'İndirimler', 'Yeni Ürünler'],
    }

    paramsSchema = ParamsSchema(**p)

    status = {
        "status": MessageStatus.INIT,
        "created_at": datetime.now(),
        "created_by": f"{SERVICE_NAME}-{WORKER_ID}",
    }

    statusSchema = StatusSchema(**status)

    message = {
        "id": str(uuid4()),
        "service": SERVICE_NAME,
        "status": [statusSchema],
        "params": paramsSchema,
    }

    ms = MessageSchema(**message)

    json_message = ms.to_json()

    yield json_message


def write_message():
    r = get_connection()
    try:
        r.xgroup_create(JOBS, REDIS_JOBS_CONSUMER_GROUP, mkstream=True)
    except Exception:
        print("Consumer group already exists, continuing")
    for message in create_message():
        key = f"{SERVICE_NAME}-{str(uuid4())}"
        r.xadd(JOBS, {key: message})
        print(f"key: {key}")
        print(f"message: {message}")
        print("")


write_message()
