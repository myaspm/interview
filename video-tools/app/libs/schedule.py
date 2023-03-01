from functools import wraps
from pytz import utc
from uuid import uuid4

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.redis import RedisJobStore
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor

from conf.config import (
    REDIS_HOST,
    REDIS_PORT,
    REDIS_DB,
    REDIS_PW,
    SERVICE_NAME,
    REDIS_JOBS_CONSUMER_GROUP,
    WORKER_ID,
    SERVICE_NAME,
)

from libs.redis_utils import am_i_leader, get_connection, write_to_stream, JOBS
from libs.scheduler_models import ScheduleType

CLUSTER = f"{SERVICE_NAME}_{REDIS_JOBS_CONSUMER_GROUP}"

# WARNING! Globale state variable.  Use with caution
scheduler = None  # BackgroundScheduler global state


def init_scheduler():
    """Short summary.

    Returns
    -------
    BackgroundScheduler
        Description of returned object.

    """
    global scheduler
    if not scheduler:
        jobstores = {
            "default": RedisJobStore(
                jobs_key=f"scheduler_{CLUSTER}_jobs",
                run_times_key=f"scheduler_{CLUSTER}_runtimes",
                host=REDIS_HOST,
                port=REDIS_PORT,
                db=REDIS_DB,
                password=REDIS_PW,
            )
        }
        executors = {"default": ProcessPoolExecutor(5)}
        job_defaults = {"coalesce": False, "max_instances": 3}

        scheduler = BackgroundScheduler(
            jobstores=jobstores,
            executors=executors,
            job_defaults=job_defaults,
            timezone=utc,
        )

    return scheduler


def control_scheduler_status(leader=False):
    """This function controls the state of the APScheduler and subscribes to
    leader_subject"""

    global scheduler
    scheduler_should_run = False
    if leader:
        scheduler_should_run = True

    if scheduler and not scheduler_should_run:
        # scheduler is alive but worker is not leader
        scheduler.shutdown()
        scheduler = None

    if scheduler_should_run:
        if not scheduler:
            # scheduler is not available and worker is leader
            scheduler = init_scheduler()
            scheduler.start()
        else:
            scheduler.wakeup()  # get jobs from jobstore


def are_jobs_scheduled(**kwargs):
    conn = kwargs.get("conn", None)
    if not conn:
        conn = get_connection()

    key = f"lock_{CLUSTER}_scheduler_jobs_initiator"
    initiator = conn.set(key, WORKER_ID, nx=True)

    if initiator:
        return False
    return True


def run_on_leader():
    """This decorater makes the function run only on leader instance

    Returns
    -------
    function -> Decorator
        Returns the decoareted function.

    """

    def decorator(original_func):
        @wraps(original_func)
        def decorated_func(*args, **kwargs):
            if am_i_leader():
                return original_func(*args, **kwargs)
            return None

        return decorated_func

    return decorator


@run_on_leader()
def send_message(message):
    """Writes message to the redis queue/stream for processing.

    Parameters
    ----------
    message : MessageSchema
        MessageSchema object to process.

    Notes
        This function should run only on leader

    """
    redis_connection = get_connection()

    if message.schedule.schedule_type == ScheduleType.DATE:
        message.update_status_init(WORKER_ID)

    elif message.schedule.schedule_type == ScheduleType.INTERVAL:
        parent_id = message.id
        message.id = str(uuid4())
        message.parent = parent_id
        message.schedule = None
        message.update_status_init(WORKER_ID)

    elif message.schedule.schedule_type == ScheduleType.CRON:
        parent_id = message.id
        message.id = str(uuid4())
        message.parent = parent_id
        message.schedule = None
        message.update_status_init(WORKER_ID)

    key = f"{SERVICE_NAME}-{str(uuid4())}"
    val = {
        "key": key,
        "val": message.to_json(),
        "conn": redis_connection,
        "stream": JOBS,
    }
    write_to_stream(**val)


def schedule_date_scheduler_message(message, scheduler):
    """Short summary.

    Parameters
    ----------
    message : MessageSchema
        MessageSchema object to schedule.
    scheduler : BackgroundScheduler
        Scheduler to execute the commands

    """
    job_id = message.id
    params = message.schedule.params

    scheduler.add_job(
        send_message,
        "date",
        run_date=params.run_date,
        misfire_grace_time=None,
        id=f"{SERVICE_NAME}-{job_id}",
        replace_existing=True,
        coalesce=True,
        kwargs={"message": message},
    )


def schedule_interval_scheduler_message(message, scheduler):
    """Short summary.

    Parameters
    ----------
    message : MessageSchema
        MessageSchema object to schedule.
    scheduler : BackgroundScheduler
        Scheduler to execute the commands

    """
    job_id = message.id
    params = message.schedule.params

    trigger_args = {}

    if params.start_date:
        trigger_args["start_date"] = params.start_date
    if params.end_date:
        trigger_args["end_date"] = params.end_date
    if params.weeks:
        trigger_args["weeks"] = params.weeks
    if params.days:
        trigger_args["days"] = params.days
    if params.hours:
        trigger_args["hours"] = params.hours
    if params.minutes:
        trigger_args["minutes"] = params.minutes
    if params.seconds:
        trigger_args["seconds"] = params.seconds
    if params.jitter:
        trigger_args["jitter"] = params.jitter

    scheduler.add_job(
        func=send_message,
        trigger="interval",
        misfire_grace_time=None,
        id=f"{SERVICE_NAME}-{job_id}",
        replace_existing=True,
        coalesce=True,
        kwargs={"message": message},
        **trigger_args,
    )


def schedule_cron_scheduler_message(message, scheduler):
    """Short summary.

    Parameters
    ----------
    message : MessageSchema
        MessageSchema object to schedule.
    scheduler : BackgroundScheduler
        Scheduler to execute the commands

    """
    job_id = message.id
    params = message.schedule.params

    trigger_args = {}

    if params.start_date:
        trigger_args["start_date"] = params.start_date
    if params.end_date:
        trigger_args["end_date"] = params.end_date
    if params.year:
        trigger_args["year"] = params.year
    if params.month:
        trigger_args["month"] = params.month
    if params.week:
        trigger_args["week"] = params.week
    if params.day_of_week:
        trigger_args["day_of_week"] = params.day_of_week
    if params.hour:
        trigger_args["hour"] = params.hour
    if params.minute:
        trigger_args["minute"] = params.minute
    if params.second:
        trigger_args["second"] = params.second
    if params.jitter:
        trigger_args["jitter"] = params.jitter

    scheduler.add_job(
        send_message,
        "cron",
        misfire_grace_time=None,
        id=f"{SERVICE_NAME}-{job_id}",
        replace_existing=True,
        coalesce=True,
        args=(message),
        **trigger_args,
    )


def schedule_message(message):
    """Schedules the given message.

    Parameters
    ----------
    message : MessageSchema
        MessageSchema object to schedule.

    """
    global scheduler
    if scheduler.state == 0:
        scheduler.start(paused=True)

    if message.schedule.schedule_type == ScheduleType.DATE:
        schedule_date_scheduler_message(message, scheduler)
    if message.schedule.schedule_type == ScheduleType.INTERVAL:
        schedule_interval_scheduler_message(message, scheduler)
    if message.schedule.schedule_type == ScheduleType.CRON:
        schedule_cron_scheduler_message(message, scheduler)

    if scheduler.state == 2:
        scheduler.shutdown()
