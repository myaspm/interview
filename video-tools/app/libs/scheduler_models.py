"""Dataclasses for Message Schedulers"""

from enum import Enum
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import Any


class ScheduleType(Enum):
    """Class that defines the params that should be implement for APScheduler.

    Attributes
    ----------
    DATE : int
        This is the simplest possible method of scheduling a job. It schedules
        a job to be executed once at the specified time. It is APScheduler’s
        equivalent to the UNIX “at” command.
    CRON : int
        Triggers when current time matches all specified time constraints,
        similarly to how the UNIX cron scheduler works.
    INTERVAL : int
        Triggers on specified intervals, starting on start_date if specified,
        datetime.now() + interval otherwise.

    """

    DATE = 0
    CRON = 1
    INTERVAL = 2


@dataclass
class CronSchema:
    """Cron Type Schema for APScheduler scheduler of the attached messeage.

    Attributes
    ----------
    start_date : datetime
        You can also specify the starting date for the cron-style schedule
        through 'start_date' parameter. They can be given as a date/datetime object
        or text (in the ISO 8601 format).
    end_date : datetime
        You can also specify the ending date for the cron-style schedule
        through 'end_date' parameter. They can be given as a date/datetime object
        or text (in the ISO 8601 format).
    year : Any -> (int|str))
        4-digit year
    month : Any -> (int|str))
        month (1-12)
    day : Any -> (int|str))
         day of month (1-31)
    week : Any -> (int|str))
         ISO week (1-53)
    day_of_week : Any -> (int|str))
         number or name of weekday (0-6 or mon,tue,wed,thu,fri,sat,sun)
    hour : Any -> (int|str))
        hour (0-23)
    minute : Any -> (int|str))
        minute (0-59)
    second : Any -> (int|str))
        second (0-59)
    jitter : int
        delay the job execution by jitter seconds at most

    Examples:
        Schedules job_function to be run on the third Friday of June, July,
        August, November and December at 00:00, 01:00, 02:00 and 03:00
        (month='6-8,11-12', day='3rd fri', hour='0-3')

        Runs from Monday to Friday at 5:30 (am) until 2014-05-30 00:00:00
        (day_of_week='mon-fri', hour=5, minute=30, end_date='2014-05-30')

        Run the `job_function` every sharp hour with an extra-delay picked
        randomly in a [-120,+120] seconds window.
        (hour='*', jitter=120)


    Notes:
        The month and day_of_week fields accept abbreviated English month and
        weekday names (jan – dec and mon – sun) respectively.

        Unlike with crontab expressions, you can omit fields that you don’t
        need. Fields greater than the least significant explicitly defined
        field default to * while lesser fields default to their minimum values
        except for week and day_of_week which default to *.

        For example, day=1, minute=20 is equivalent to year='*', month='*',
        day=1, week='*', day_of_week='*', hour='*', minute=20, second=0. The
        job will then execute on the first day of every month on every year at
        20 minutes of every hour.

    """

    start_date: datetime = field(default=datetime.utcnow())
    end_date: datetime = field(default=datetime.utcnow() + timedelta(days=365))
    year: Any = field(default=None)
    month: Any = field(default=None)
    week: Any = field(default=None)
    day_of_week: Any = field(default=None)
    hour: Any = field(default=None)
    minute: Any = field(default=None)
    second: Any = field(default=None)
    jitter: int = field(default=None)


@dataclass
class IntervalSchema:
    """Interval Type Schema for APScheduler scheduler of the attached messeage.

    Attributes
    ----------
    start_date : datetime
        You can also specify the starting date for the cron-style schedule
        through 'start_date' parameter. They can be given as a date/datetime object
        or text (in the ISO 8601 format).
    end_date : datetime
        You can also specify the ending date for the cron-style schedule
        through 'end_date' parameter. They can be given as a date/datetime object
        or text (in the ISO 8601 format).
    weeks : int
        number of weeks to wait
    days : int
        number of days to wait
    hours : int
        number of hours to wait
    minutes : int
        number of minutes to wait
    seconds : int
        number of seconds to wait
    jitter : int
        delay the job execution by jitter seconds at most

    Examples:
        Schedule job_function to be called every two hours
        (hours=2)

        The same as before, but starts on 2010-10-10 at 9:30 and stops on
        2014-06-15 at 11:00
        (hours=2, start_date='2010-10-10 09:30:00',
        end_date='2014-06-15 11:00:00')

        Run the `job_function` every hour with an extra-delay picked randomly
        in a [-120,+120] seconds window.
        (hours='1', jitter=120)


    Notes:
        This method schedules jobs to be run periodically, on selected
        intervals.

        You can also specify the starting date and ending dates for the
        schedule through the start_date and end_date parameters, respectively.
        They can be given as a date/datetime object or text
        (in the ISO 8601 format).

        If the start date is in the past, the trigger will not fire many times
        retroactively but instead calculates the next run time from the current
        time, based on the past start time.

    """

    start_date: datetime = field(default=datetime.utcnow())
    end_date: datetime = field(default=datetime.utcnow() + timedelta(days=365))
    weeks: int = field(default=None)
    days: int = field(default=None)
    hours: int = field(default=None)
    minutes: int = field(default=None)
    seconds: int = field(default=None)
    jitter: int = field(default=None)


@dataclass
class DateSchema:
    """Date Type Schema for APScheduler scheduler of the attached messeage.

    Attributes
    ----------
    run_date : datetime
        the date/time to run the job at

    Examples:
        The job will be executed on November 6th, 2009 at 16:30:05
        (run_date=datetime(2009, 11, 6, 16, 30, 5))

    Notes:
        This is the simplest possible method of scheduling a job. It schedules
        a job to be executed once at the specified time. It is APScheduler’s
        equivalent to the UNIX “at” command.

        The run_date can be given either as a date/datetime object or text
        (in the ISO 8601 format).

    """

    run_date: datetime = field(default=datetime.utcnow())


@dataclass
class ScheduleSchema:
    """Schema for APScheduler scheduler of the attached messeage.

    Attributes
    ----------
    schedule_type : ScheduleType
        Select the schedule type for the scheduler.
        Defaults to 'ScheduleType.DATE'
    params : Any
        Paramater schema for selected schedule_type
        Valid Options are: CronSchema, DateSchema, IntervalSchema

    """

    schedule_type: ScheduleType = field(default=ScheduleType.DATE)
    params: Any = field(default=None)

    def convert_params_schema(self):
        """Converts dicts to respective data classes"""
        self.schedule_type = ScheduleType(self.schedule_type)

        if self.schedule_type == ScheduleType.CRON:
            self.params = CronSchema(**self.params)

        if self.schedule_type == ScheduleType.INTERVAL:
            self.params = IntervalSchema(**self.params)

        if self.schedule_type == ScheduleType.DATE:
            self.params = DateSchema(**self.params)
