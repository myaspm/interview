from uuid import uuid4
from os import environ

SERVICE_NAME = "playwright-parser-service"
WORKER_ID = str(uuid4())

# Dev Config
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 0
REDIS_PW = "redismaster"
REDIS_JOBS_CONSUMER_GROUP = "pwp_jobs_grp"
READ_N = 1

