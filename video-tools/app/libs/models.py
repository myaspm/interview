from enum import Enum
from datetime import datetime

from dataclasses import dataclass, field
from typing import List, Any

import orjson as json

from libs.redis_utils import set_message
from libs.scheduler_models import ScheduleSchema  # , ScheduleType


class MessageStatus(Enum):
    INIT = 0
    PROCESSING = 1
    SUCCESS = 2
    FAILURE = 3
    SCHEDULED = 4


@dataclass
class ErrorSchema:
    reason: str
    created_by: str
    created_at: datetime = field(default=datetime.now())


@dataclass
class StatusSchema:
    status: MessageStatus
    created_by: str
    created_at: datetime = field(default=datetime.now())


@dataclass
class MessageSchema:
    id: str
    service: str
    parent: str = field(default=None)
    error: ErrorSchema = field(default=None)
    status: List[StatusSchema] = field(default_factory=list)
    schedule: ScheduleSchema = field(default=None)
    params: Any = field(default=None)
    response: Any = field(default=None)

    def set_status(self, messageStatus, created_by):
        s = StatusSchema(messageStatus, created_by)
        self.status.append(s)

    def set_error(self, err, created_by):
        self.error = ErrorSchema(err, created_by)

    def to_json(self):
        return json.dumps(self)

    @classmethod
    def from_json(cls, json_data):
        data_dict = json.loads(json_data)
        message = cls(**data_dict)
        message.convert_essential_schemas()
        if message.params:
            message.convert_params()
        if message.response:
            message.convert_response()
        return message

    def convert_params(self):
        raise NotImplementedError

    def convert_response(self):
        raise NotImplementedError

    def update(self):
        set_message(self)

    def update_status(self, status, worker_id):
        ss = StatusSchema(status, worker_id)
        self.status.append(ss)
        self.update()

    def convert_error_schema(self):
        if self.error:
            self.error = ErrorSchema(**self.error)

    def convert_status_schema(self):
        self.status = [StatusSchema(**status) for status in self.status]
        for status in self.status:
            status.status = MessageStatus(status.status)

    def convert_schedule_schema(self):
        if self.schedule:
            # print(self.schedule)
            # self.schedule["schedule_type"] = ScheduleType(
            #     self.schedule["schedule_type"]
            # )
            self.schedule = ScheduleSchema(**self.schedule)
            self.schedule.convert_params_schema()

    def convert_essential_schemas(self):
        self.convert_error_schema()
        self.convert_status_schema()
        self.convert_schedule_schema()

    def update_status_init(self, worker_id):
        self.update_status(MessageStatus.INIT, worker_id)

    def update_status_processing(self, worker_id):
        self.update_status(MessageStatus.PROCESSING, worker_id)

    def update_status_success(self, worker_id):
        self.update_status(MessageStatus.SUCCESS, worker_id)

    def update_status_failure(self, worker_id):
        self.update_status(MessageStatus.FAILURE, worker_id)

    def update_status_scheduled(self, worker_id):
        self.update_status(MessageStatus.SCHEDULED, worker_id)

    def error_message(self, reason, worker_id):
        error_schema = ErrorSchema(str(reason), worker_id)
        self.error = error_schema
        self.update_status_failure(worker_id)

    def get_latest_status(self):
        return self.status[-1].status

    def is_status_init(self):
        latest = self.get_latest_status()
        return latest == MessageStatus.INIT

    def is_scheduled(self):
        latest = self.get_latest_status()
        return latest == MessageStatus.SCHEDULED
