from uuid import uuid4
from datetime import datetime
from libs.models import (
    ParamsSchema,
    StatusSchema,
    TtsMessageScheme as MessageSchema,
    MessageStatus,
)

from config.config import SERVICE_NAME, REDIS_JOBS_CONSUMER_GROUP, WORKER_ID
from libs.redis_utils import get_connection, JOBS, write_to_stream


def create_message():

    p = {
        "text": "Artık temel bir insan hakkı olan internetten hiçbir çocuğumuz mahrum kalmasın diye yola çıktık. Bugün köylerimizde başlattığımız ücretsiz internet hizmetimizin 1. yılını kutluyoruz.",
        "voicename": "tr-TR-Wavenet-A",
        "pitch": -2.40,
        "speed": 1.19,
        "language": "tr-TR",
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
        print("Creating consumer group")
        print(r)
        r.xgroup_create(JOBS, REDIS_JOBS_CONSUMER_GROUP, mkstream=True)
        print("Consumer group created")
    except Exception:
        print("Consumer group already exists, continuing")

    for message in create_message():
        key = f"{SERVICE_NAME}-{str(uuid4())}"
        m = {"key": key, "val": message, "conn": r, "stream": JOBS}
        write_to_stream(**m)
        print(f"key: {key}")
        print(f"message: {message}")
        print("")


write_message()
