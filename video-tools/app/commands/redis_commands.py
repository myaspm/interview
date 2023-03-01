from termcolor import cprint
from nubia import argument, command

from conf.config import (
    SERVICE_NAME,
    WORKER_ID,
)


from libs.redis_utils import scan_redis_keys


@command
@argument("pattern", description="Redis key scan pattern")
def scan_keys(pattern=None):
    """Register jobs declared in service.scheduled_jobs.jobs"""

    if not pattern:
        pattern = f"{SERVICE_NAME}*"
    for key in scan_redis_keys(pattern):
        cprint(key)

    return 0
