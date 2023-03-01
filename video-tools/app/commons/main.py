"""Short summary.

Notes
-----
    Short summary.

"""
from conf.config import WORKER_ID, EMIT_SUCCESS_EVENT

from conf.log import log
from libs.redis_utils import ack_message
from libs.service_utils import set_working
from libs.schedule import schedule_message
from service.libs.models import VidToolsMessageScheme as BaseScheme
from service.logic import process_message
from commons.subjects import event_subject as es


def convert_message(msg, key=None):
    """Converts the JSON into MessageSchema DataClass.

    Parameters
    ----------
    msg : str -> JSON
        Description of parameter `msg`.
    key : str
        Description of parameter `key`.

    Returns
    -------
    MessageSchema
        Description of returned object.

    """
    try:
        message_scheme = BaseScheme.from_json(msg)
        log_msg = f"[Stream ID: {id}] [Key: {key}]"
        log_msg = f"{log_msg} [Message: {message_scheme.id}] Message recieved"
        log.debug(log_msg)
        return message_scheme
    except Exception as exc:
        log_msg = f"Message cannot be read - Message ID:{id}"
        log_msg = f"[Stream ID: {id}] {log_msg} Reason: {exc}"
        log.error(log_msg)
        log.exception(log_msg)

        return None


def ack_received_message(message_id=id):
    """Acknowledge received message with consumer group id

    Parameters
    ----------
    id : str
        Description of parameter `id`.

    """
    ack_result = ack_message(id=message_id)
    if ack_result:
        log_msg = "Message is acknowledged"
        log_msg = f"[Stream ID: {id}] {log_msg}"
        log.debug(log_msg)
    else:
        log_msg = f"Message cannot be acknowledged - Message ID:{id}"
        log_msg = f"[Stream ID: {id}] {log_msg}"
        log.error(log_msg)


def process_scheduled_message(message_scheme):
    try:

        schedule_message(message_scheme)
        log_msg = f"[Message: {message_scheme.id}] scheduled"
        log.info(log_msg)
    except Exception as exc:
        log_msg = "Message is failed to schedule"
        log_msg = f"[Message: {message_scheme.id}] {log_msg} Reason: {exc}"
        log.error(log_msg)
        log.exception(log_msg)


def process_received_message(
    message_scheme,
    worker=WORKER_ID,
    emit_success=EMIT_SUCCESS_EVENT,
    event_subject=es,
):
    try:
        # main logic entry

        # update worker status
        set_working(message_scheme.id)
        resp = process_message(message_scheme.params)
        if resp:
            # if there is a response
            # it will be added to the original messge
            message_scheme.set_response(resp)
        message_scheme.update_status_success(worker)
        log_msg = "Message status updated as success"
        log_msg = f"[Message: {message_scheme.id}] {log_msg}"
        log.debug(log_msg)
        if emit_success:
            event_subject.on_next(message_scheme)

    except Exception as exc:
        log_msg = f"[Message: {message_scheme.id}] Reason: {exc}"
        log.error(log_msg)
        message_scheme.error_message(exc, worker)
        log_msg = "Error occured during process."
        log_msg = f"[Message: {message_scheme.id}] {log_msg}"
        log.error(log_msg)


def do_logic(message):
    """Short summary.

    Parameters
    ----------
    message : MessageSchema
        Description of parameter `message`.

    """
    message_id = message[0]
    key = message[1]
    msg = message[2]

    message_scheme = convert_message(msg, key)
    if not message_scheme:
        return None

    # check if the message is in init status
    if message_scheme.is_status_init():
        message_scheme.update_status_processing(WORKER_ID)
        log_msg = "Message is being processed"
        log_msg = f"[Message: {message_scheme.id}] {log_msg}"
        log.debug(log_msg)

        # acknowledge that messace is received
        ack_received_message(message_id=message_id)

        # process the message
        process_received_message(
            message_scheme, WORKER_ID, EMIT_SUCCESS_EVENT, es
        )

    elif message_scheme.is_scheduled():
        log_msg = "Message is set for scheduling"
        log_msg = f"[Message: {message_scheme.id}] {log_msg}"
        log.debug(log_msg)
        process_scheduled_message(message_scheme)

    else:
        # message is not in init status but an unacked message recieved
        log_msg = "Unacked message received "
        log_msg = f"Stream ID: {message_id}] {log_msg} : {message_scheme}"
        log.warning(log_msg)
        ack_received_message(message_id=message_id)

    return None
