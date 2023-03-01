"""Short summary.

Notes
-----
    Short summary.

"""

from os import mkdir
from os.path import isdir

from conf.config import TEMP_DIR

from service.libs.video_operations import (
    color_clip,
    image_clip,
    encode_clip,
    text_overlay_clip,
    concat_clips,
    subclip,
    create_hls,
    sound_mix,
    create_frame,
    create_tts,
    create_pvi
)
from service.libs.models import JobType


def process_message(params):
    """main logic entry
       raise am exception here to mark the message with error_message
       the message will be marked as success if no exception is raised

    Parameters
    ----------
    params : Dict
        Description of parameter `params`.

    Returns
    -------
    ResponseSchema
        Description of returned object.

    """

    # Check temp dir:
    if not isdir(TEMP_DIR):
        mkdir(TEMP_DIR)

    response = None
    try:
        if params.job == JobType.COLOR:
            response = color_clip(params.job_params)

        if params.job == JobType.IMAGE:
            response = image_clip(params.job_params)

        if params.job == JobType.TEXT:
            response = text_overlay_clip(params.job_params)

        if params.job == JobType.ENCODE:
            response = encode_clip(params.job_params)

        if params.job == JobType.CONCAT:
            response = concat_clips(params.job_params)

        if params.job == JobType.SUBCLIP:
            response = subclip(params.job_params)

        if params.job == JobType.HLS:
            response = create_hls(params.job_params)

        if params.job == JobType.SOUNDMIX:
            response = sound_mix(params.job_params)

        if params.job == JobType.FRAME:
            response = create_frame(params.job_params)

        if params.job == JobType.TTS:
            response = create_tts(params.job_params)

        if params.job == JobType.PVI:
            response = create_pvi(params.job_params)


        if response:
            return response

        raise Exception("JOB FAILED: No response object.")

    except Exception as e:
        raise e

    return None
