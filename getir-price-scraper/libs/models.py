from enum import Enum
from datetime import datetime

from dataclasses import dataclass, field
from typing import List

import orjson as json

from .redis_utils import set_message


class MessageStatus(Enum):
    INIT = 0
    PROCESSING = 1
    SUCCESS = 2
    FAILURE = 3


@dataclass
class ParamsSchema:
    blacklisted_cats: list


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
    params: ParamsSchema = field(default_factory=ParamsSchema)
    error: ErrorSchema = field(default=None)
    status: List[StatusSchema] = field(default_factory=list)

    def set_status(self, messageStatus, created_by):
        s = StatusSchema(messageStatus, created_by)
        self.status.append(s)

    def set_error(self, err, created_by):
        self.error = ErrorSchema(err, created_by)

    def to_json(self):
        return json.dumps(self)

    def update(self):
        set_message(self)

    def update_status(self, status, worker_id):
        ss = StatusSchema(status, worker_id)
        self.status.append(ss)
        self.update()

    def update_status_init(self, worker_id):
        self.update_status(MessageStatus.INIT, worker_id)

    def update_status_processing(self, worker_id):
        self.update_status(MessageStatus.PROCESSING, worker_id)

    def update_status_success(self, worker_id):
        self.update_status(MessageStatus.SUCCESS, worker_id)

    def update_status_failure(self, worker_id):
        self.update_status(MessageStatus.FAILURE, worker_id)

    def error_message(self, reason, worker_id):
        es = ErrorSchema(str(reason), worker_id)
        self.error = es
        self.update_status_failure(worker_id)

    def get_latest_status(self):
        return self.status[-1].status

    def is_status_init(self):
        latest = self.get_latest_status()
        return latest == MessageStatus.INIT


def get_messagescheme_from_json(data):
    ms = MessageSchema(**json.loads(data))
    ms.params = ParamsSchema(**ms.params)
    if ms.error:
        ms.params = ErrorSchema(**ms.error)
    ms.status = [StatusSchema(**s) for s in ms.status]
    for ss in ms.status:
        ss.status = MessageStatus(ss.status)

    return ms
