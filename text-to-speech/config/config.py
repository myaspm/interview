from uuid import uuid4
from os import environ

SERVICE_NAME = "text-to-speech"
WORKER_ID = str(uuid4())

# Dev Config
# REDIS_HOST = "localhost"
# REDIS_PORT = 6379
# REDIS_DB = 0

# Prod Config
REDIS_HOST = environ.get("REDIS_HOST", "127.0.0.1")
REDIS_PORT = environ.get("REDIS_PORT", 6379)
REDIS_DB = environ.get("REDIS_DB", 0)
REDIS_PW = environ.get("REDIS_PW", "redismaster")

REDIS_JOBS_CONSUMER_GROUP = "tts_jobs_grp"
REDIS_JOB_POLL_BLOCK = 6000

READ_N = 1

AWS_ACCESS_SETTINGS = {
    "endpoint_url": "http://127.0.0.1:9000",
    "aws_access_key_id": "DUL0PFENNSC7TK5S9ELQ",
    "aws_secret_access_key": "x4mGd060HHR0W0NKGGoAVlye9nFE2MBKbTEUVaMa",
    "region_name": None
}
