"""Short summary.

Notes
-----
    Short summary.

"""


from uuid import uuid4
from os import environ

SERVICE_NAME = "vid-tools"
WORKER_ID = str(uuid4())
HAS_SCHEDULER = True
STREAM_STATUS = True
EMIT_SUCCESS_EVENT = True
SUCCESS_EVENT_STREAM = f"stream_{SERVICE_NAME}_messages_success"
GROUP_ID = "1"
WORKER_HEARTHBEAT_TTL = 60 * 60 * 24  # 1 day TTL


# Dev Config
# REDIS_HOST = environ.get("REDIS_HOST", "192.168.0.44")
# REDIS_PORT = environ.get("REDIS_PORT", 6379)
# REDIS_DB = environ.get("REDIS_DB", 0)
# REDIS_PW = environ.get("REDIS_PW", "")

REDIS_JOBS_CONSUMER_GROUP = environ.get(
    "REDIS_JOBS_CONSUMER_GROUP", f"consumergroup_{GROUP_ID}"
)
REDIS_JOB_POLL_BLOCK = 6000

READ_N = int(environ.get("READ_N", 1))

# Dev Config
# S3_ENDPOINT_URL = environ.get("S3_ENDPOINT_URL", "http://192.168.0.44:9000")
# S3_ACCESS_KEY_ID = environ.get("S3_ACCESS_KEY_ID", "12345678")
# S3_SECRET_ACCESS_KEY = environ.get("S3_SECRET_ACCESS_KEY", "12345678")
# S3_REGION_NAME = environ.get("S3_REGION_NAME", None)

LOG_REDIS = True
LOG_STREAM = "stream_service-logs"
LOG_REDIS_HOST = environ.get("LOG_REDIS_HOST", REDIS_HOST)
LOG_REDIS_PORT = environ.get("LOG_REDIS_PORT", REDIS_PORT)
LOG_REDIS_DB = environ.get("LOG_REDIS_DB", 14)
LOG_REDIS_PW = environ.get("LOG_REDIS_PW", REDIS_PW)

AWS_ACCESS_SETTINGS = {
    "endpoint_url": S3_ENDPOINT_URL,
    "aws_access_key_id": S3_ACCESS_KEY_ID,
    "aws_secret_access_key": S3_SECRET_ACCESS_KEY,
    "region_name": S3_REGION_NAME,
}
DEFAULT_BUCKET = SERVICE_NAME
TEMP_DIR = "tmp_dir"
