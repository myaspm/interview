from libs.models import get_messagescheme_from_json
from config import WORKER_ID

from log import log
from libs.redis_utils import ack_message
from logic import process_message


def do_logic(message):
    id = message[0]
    key = message[1]
    msg = message[2]

    ms = get_messagescheme_from_json(msg)
    log.info(
        f"[Stream ID: {id}] [Key: {key}] [Message: {ms.id}] Message recieved"
    )

    # check if the message is in init status
    if ms.is_status_init():
        ms.update_status_processing(WORKER_ID)
        log_msg = "Message is being processed"
        log.info(f"[Worker: {WORKER_ID}] [Message: {ms.id}] {log_msg}")
        ack_result = ack_message(id=id)
        if ack_result:
            log_msg = "Message is acknowledged"
            log.info(f"Stream ID: {id}] [Worker: {WORKER_ID}] {log_msg}")
        else:
            log_msg = " Message cannot be acknowledged"
            log.error(f"Stream ID: {id}] [Worker: {WORKER_ID}] {log_msg}")

        try:
            # main logic entry
            process_message(ms.params)
            ms.update_status_success(WORKER_ID)
            log_msg = "Message status updated as success"
            log.info(f"[Worker: {WORKER_ID}] [Message: {ms.id}] {log_msg}")

        except Exception as e:
            log.error(f"[Worker: {WORKER_ID}] [Message: {ms.id}] {e}")
            ms.error_message(e, WORKER_ID)
            log_msg = "Error occured during process."
            log.error(f"[Worker: {WORKER_ID}] [Message: {ms.id}] {log_msg}")

    else:
        # message is not in init status but recieved
        # unacked message
        log_msg = "Unacked message"
        log.warning(f"Stream ID: {id}] [Worker: {WORKER_ID}] {log_msg} : {ms}")
        ack_result = ack_message(id=id)
        if ack_result:
            log_msg = "Message is acknowledged"
            log.info(f"Stream ID: {id}] [Worker: {WORKER_ID}] {log_msg}")
        else:
            log_msg = " Message cannot be acknowledged"
            log.error(f"Stream ID: {id}] [Worker: {WORKER_ID}] {log_msg}")
