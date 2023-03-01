from config.config import WORKER_ID

from libs.tts_utils import text_to_speech

from log.log import log


def process_message(params):
    # main logic entry
    # raise am exception here to mark the message with error_message
    # the message will be marked as success if no exception is raised
    return text_to_speech(params)
