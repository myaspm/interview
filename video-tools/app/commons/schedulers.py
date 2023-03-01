"""Short summary.

Notes
-----
    Short summary.

"""
from libs.schedule import run_on_leader, init_scheduler
from conf.log import log


SCHEDULER_RUNNING = None


@run_on_leader()
def start_scheduler():
    """Short summary.

    Returns
    -------
    APScheduler
        Description of returned object.

    """
    global SCHEDULER_RUNNING
    if not SCHEDULER_RUNNING:
        try:
            log.debug("Initiating Scheduler")
            scheduler = init_scheduler()
            scheduler.start()
            log.info("Scheduler Started")
            SCHEDULER_RUNNING = scheduler
            return scheduler
        except Exception as exc:
            msg = f"Scheduler failed to start [Reason:{exc}]"
            log.error(msg)
    else:

        log.info("Scheduler is already started")
        SCHEDULER_RUNNING.wakeup()
        return SCHEDULER_RUNNING

    return None
