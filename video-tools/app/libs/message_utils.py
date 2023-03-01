from conf.config import WORKER_ID
from libs.models import StatusSchema, ErrorSchema, MessageStatus


def update_message_status(msg, status):
    ss = StatusSchema(StatusSchema, WORKER_ID)
    msg.status.append(ss)
    msg.update()


def error_message(msg, reason):
    es = ErrorSchema(reason, WORKER_ID)
    msg.error = es
    update_message_status(msg, MessageStatus.FAILURE)
