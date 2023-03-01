from commands.register_scheduled_jobs import register_jobs
from conf.log import log

if __name__ == "__main__":
    register_jobs()
    log.info("Jobs are scheduled")
