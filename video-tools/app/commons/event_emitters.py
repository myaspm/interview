from conf.log import log

from conf.config import SERVICE_NAME, SUCCESS_EVENT_STREAM
from libs.redis_utils import get_connection, write_to_stream


def emit_event(message=None):
    conn = get_connection()
    key = f"{SERVICE_NAME}_{message.id}"
    stream_message = {
        "key": key,
        "val": message.to_json(),
        "conn": conn,
        "stream": SUCCESS_EVENT_STREAM,
    }
    try:
        write_to_stream(**stream_message)
        log_msg = f"Success event emitted for {message.id}"
        log.info(log_msg)
    except Exception as exc:
        log_msg = f"Error in event emit for {message.id} | Reason: {exc}"
        log.error(log_msg)
