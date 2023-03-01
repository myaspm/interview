from termcolor import cprint
from nubia import argument, command

from conf.log import log
from libs.schedule import init_scheduler
from service.scheduled_jobs.jobs import add_jobs


def get_paused_scheduler():
    """Short summary.

    Returns
    -------
    BackgroundScheduler
        Description of returned object.

    """
    log.info("Initiating Scheduler")
    scheduler = init_scheduler()
    scheduler.start(paused=True)
    return scheduler


@command
def register_jobs():
    """Register jobs declared in service.scheduled_jobs.jobs"""
    scheduler = get_paused_scheduler()
    add_jobs(scheduler)
    scheduler.shutdown()

    return 0


@command
def list_jobs():
    """List all scheduled jobs"""
    scheduler = get_paused_scheduler()
    for job in scheduler.get_jobs():
        cprint(str(job))
    scheduler.shutdown()

    return 0


@command
def remove_all_jobs():
    """Remove all scheduled jobs"""
    scheduler = get_paused_scheduler()
    for job in scheduler.get_jobs():
        cprint(str(job))
        scheduler.remove_job(job.id)
    scheduler.shutdown()

    return 0
