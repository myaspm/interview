from datetime import datetime, timedelta
from uuid import uuid4
from libs.schedule import run_on_leader
from conf.config import SERVICE_NAME, WORKER_ID


@run_on_leader()
def test_func():
    print("I run on the leader baby and I am scheduled")


def test_jobs(scheduler):
    alarm_time = datetime.utcnow() + timedelta(seconds=5)
    scheduler.add_job(
        test_func,
        "date",
        run_date=alarm_time,
        misfire_grace_time=None,
        id=f"{SERVICE_NAME}-1",
        replace_existing=True,
        coalesce=True,
    )
    alarm_time = datetime.utcnow() + timedelta(seconds=10)
    scheduler.add_job(
        test_func,
        "date",
        run_date=alarm_time,
        misfire_grace_time=None,
        id=f"{SERVICE_NAME}-2",
        replace_existing=True,
        coalesce=True,
    )
    # alarm_time = datetime.utcnow() + timedelta(seconds=15)
    # scheduler.add_job(
    #     test_func,
    #     "date",
    #     run_date=alarm_time,
    #     misfire_grace_time=None,
    #     id=f"{SERVICE_NAME}-{str(uuid4())}",
    #     replace_existing=True,
    #     coalesce=True,
    # )
    # alarm_time = datetime.utcnow() + timedelta(seconds=25)
    # scheduler.add_job(
    #     test_func,
    #     "date",
    #     run_date=alarm_time,
    #     misfire_grace_time=None,
    #     id=f"{SERVICE_NAME}-{str(uuid4())}",
    #     replace_existing=True,
    #     coalesce=True,
    # )
    # alarm_time = datetime.utcnow() + timedelta(seconds=35)
    # scheduler.add_job(
    #     test_func,
    #     "date",
    #     run_date=alarm_time,
    #     misfire_grace_time=None,
    #     id=f"{SERVICE_NAME}-{str(uuid4())}",
    #     replace_existing=True,
    #     coalesce=True,
    # )


def add_jobs(scheduler):
    test_jobs(scheduler)
