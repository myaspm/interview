"""Short summary.

Notes
-----
    Short summary.

"""

from uuid import uuid4
from random import randrange
from datetime import datetime, timedelta
from libs.scheduler_models import (
    ScheduleType,
    IntervalSchema,
    DateSchema,
    ScheduleSchema,
)

from libs.models import (
    MessageStatus,
    StatusSchema,
)

from service.libs.models import (
    JobType,
    ReturnFileOptions,
    ParamsSchema,
    VidToolsMessageScheme as MessageSchema,
    VideoOptions,
    OverlayOptions,
    ColorClipParams,
    S3File,
    EncodeParams,
    ImageClipParams,
    TextClipParams,
    ConcatParams,
    SubClipParams,
    HLSParams,
    SoundMixClipParams,
    FrameParams,
    TTSParams,
    PviParams,
    CustomTs
)

from conf.config import (
    SERVICE_NAME,
    REDIS_JOBS_CONSUMER_GROUP,
    WORKER_ID,
    DEFAULT_BUCKET,
)
from libs.redis_utils import get_connection, JOBS, write_to_stream

message_list = list()


def create_colorclip_message():
    return_file_options = ReturnFileOptions(
        key_prefix="color", bucket=DEFAULT_BUCKET
    )
    video_options = VideoOptions(
        video_size=(640, 480), duration=3.0, fps=24, codec="libx264"
    )
    color_clip_params = ColorClipParams(
        color=(randrange(255), randrange(255), randrange(255)),
        video_options=video_options,
        output_options=return_file_options,
    )

    params_schema = ParamsSchema(
        job=JobType.COLOR, job_params=color_clip_params
    )

    # status = {
    #     "status": MessageStatus.INIT,
    #     "created_at": datetime.now(),
    #     "created_by": f"{SERVICE_NAME}-{WORKER_ID}",
    # }
    # status_schema = StatusSchema(**status)
    # run_date = datetime.now() + timedelta(seconds=15)
    # date_schema = DateSchema(run_date=run_date)
    # schedule_schema = ScheduleSchema(
    #     schedule_type=ScheduleType.DATE, params=date_schema
    # )

    message = {
        "id": str(uuid4()),
        "service": SERVICE_NAME,
        "params": params_schema,
    }

    msg = MessageSchema(**message)
    # msg.update_status_scheduled(WORKER_ID)
    msg.update_status_init(WORKER_ID)

    json_message = msg.to_json()

    return json_message


def create_imageclip_message(bucket, key, overlay_key=None):
    key_prefix = "image_clips"

    if overlay_key:
        key_prefix = "image_overlay_clips"

    return_file_options = ReturnFileOptions(
        key_prefix=key_prefix, bucket=DEFAULT_BUCKET
    )

    video_options = VideoOptions(
        video_size=(720, 480),
        duration=10.0,
        fps=25,
        codec="libx264",
        codec_extras="veryfast",
    )

    image_file = S3File(bucket=bucket, key=key)

    overlay_options = None

    if overlay_key:
        overlay_options = OverlayOptions(
            video_file=S3File(bucket=bucket, key=overlay_key),
            position=(10, 10),
            size=(250, 250),
            t_start=10.0,
            t_end=20.0,
        )

    encode_clip_params = ImageClipParams(
        image_file=image_file,
        video_options=video_options,
        output_options=return_file_options,
        overlay_options=overlay_options,
    )

    params_schema = ParamsSchema(
        job=JobType.IMAGE, job_params=encode_clip_params
    )

    status = {
        "status": MessageStatus.INIT,
        "created_at": datetime.now(),
        "created_by": f"{SERVICE_NAME}-{WORKER_ID}",
    }
    status_schema = StatusSchema(**status)

    message = {
        "id": str(uuid4()),
        "service": SERVICE_NAME,
        "status": [status_schema],
        "params": params_schema,
    }

    msg = MessageSchema(**message)

    json_message = msg.to_json()

    return json_message


def create_textoverlayclip_message(bucket, key):
    return_file_options = ReturnFileOptions(
        key_prefix="text_clips", bucket=DEFAULT_BUCKET
    )
    video_options = VideoOptions(
        video_size=(1920, 1080),
        duration=20,
        fps=24,
        codec="libx264",
        codec_extras="veryfast",
    )

    video_file = S3File(bucket=bucket, key=key)

    overlay_options = OverlayOptions(video_file=video_file, position=(20, 20))

    encode_clip_params = TextClipParams(
        text="CreoVideo Rules",
        color=(randrange(255), randrange(255), randrange(255)),
        text_size=55,
        font=S3File(bucket="musta", key="Phetsarath_OT.ttf"),
        overlay_options=overlay_options,
        video_options=video_options,
        output_options=return_file_options,
    )

    params_schema = ParamsSchema(
        job=JobType.TEXT, job_params=encode_clip_params
    )

    status = {
        "status": MessageStatus.INIT,
        "created_at": datetime.now(),
        "created_by": f"{SERVICE_NAME}-{WORKER_ID}",
    }
    status_schema = StatusSchema(**status)

    message = {
        "id": str(uuid4()),
        "service": SERVICE_NAME,
        "status": [status_schema],
        "params": params_schema,
    }

    msg = MessageSchema(**message)

    json_message = msg.to_json()

    return json_message


def create_soundclip_message(bucket, key):
    return_file_options = ReturnFileOptions(
        key_prefix="sound_mix_clips", bucket=DEFAULT_BUCKET
    )
    video_options = VideoOptions(
        video_size=None,
        duration=None,
        fps=25,
        codec="libx264",
        codec_extras="veryfast",
    )

    video_file = S3File(bucket=bucket, key=key)

    overlay_options = OverlayOptions(video_file=video_file, position=(20, 20))

    sound_clip_params = SoundMixClipParams(
        sound_file=S3File(bucket="musta", key="mustaa.wav"),
        sound_start=2.0,
        overlay_options=overlay_options,
        video_options=video_options,
        output_options=return_file_options,
    )

    params_schema = ParamsSchema(
        job=JobType.SOUNDMIX, job_params=sound_clip_params
    )

    status = {
        "status": MessageStatus.INIT,
        "created_at": datetime.now(),
        "created_by": f"{SERVICE_NAME}-{WORKER_ID}",
    }
    status_schema = StatusSchema(**status)

    message = {
        "id": str(uuid4()),
        "service": SERVICE_NAME,
        "status": [status_schema],
        "params": params_schema,
    }

    msg = MessageSchema(**message)

    json_message = msg.to_json()

    return json_message


def create_encodeclip_message(bucket, key):
    return_file_options = ReturnFileOptions(
        key_prefix="encoded_clips", bucket=DEFAULT_BUCKET
    )
    video_options = VideoOptions(
        video_size=(1920, 1080),
        fps=None,
        codec="libx264",
        codec_extras="veryfast",
    )

    video_file = S3File(bucket=bucket, key=key)

    encode_clip_params = EncodeParams(
        video_file=video_file,
        video_options=video_options,
        output_options=return_file_options,
    )

    params_schema = ParamsSchema(
        job=JobType.ENCODE, job_params=encode_clip_params
    )

    status = {
        "status": MessageStatus.INIT,
        "created_at": datetime.now(),
        "created_by": f"{SERVICE_NAME}-{WORKER_ID}",
    }
    status_schema = StatusSchema(**status)

    message = {
        "id": str(uuid4()),
        "service": SERVICE_NAME,
        "status": [status_schema],
        "params": params_schema,
    }

    msg = MessageSchema(**message)

    json_message = msg.to_json()

    return json_message


def create_concatclip_message(file_list):
    return_file_options = ReturnFileOptions(
        key_prefix="concat_clips", bucket=DEFAULT_BUCKET
    )
    video_options = VideoOptions(
        video_size=None,
        duration=None,
        fps=None,
        codec=None,
        codec_extras="veryfast",
    )

    video_file_list = [
        S3File(bucket=i["bucket"], key=i["key"]) for i in file_list
    ]

    concat_clip_params = ConcatParams(
        video_files=video_file_list,
        video_options=video_options,
        output_options=return_file_options,
    )

    params_schema = ParamsSchema(
        job=JobType.CONCAT, job_params=concat_clip_params
    )

    status = {
        "status": MessageStatus.INIT,
        "created_at": datetime.now(),
        "created_by": f"{SERVICE_NAME}-{WORKER_ID}",
    }
    status_schema = StatusSchema(**status)

    message = {
        "id": str(uuid4()),
        "service": SERVICE_NAME,
        "status": [status_schema],
        "params": params_schema,
    }

    msg = MessageSchema(**message)

    json_message = msg.to_json()

    return json_message


def create_subclip_message(bucket, key):
    return_file_options = ReturnFileOptions(
        key_prefix="subclips", bucket=DEFAULT_BUCKET
    )

    video_options = VideoOptions(
        video_size=None,
        duration=None,
        fps=None,
        codec="libx264",
        codec_extras="veryfast",
    )

    video_file = S3File(bucket=bucket, key=key)

    subclip_params = SubClipParams(
        start=20,
        end=45,
        video_file=video_file,
        video_options=video_options,
        output_options=return_file_options,
        keyframe=True,
    )

    params_schema = ParamsSchema(
        job=JobType.SUBCLIP, job_params=subclip_params
    )

    status = {
        "status": MessageStatus.INIT,
        "created_at": datetime.now(),
        "created_by": f"{SERVICE_NAME}-{WORKER_ID}",
    }
    status_schema = StatusSchema(**status)

    message = {
        "id": str(uuid4()),
        "service": SERVICE_NAME,
        "status": [status_schema],
        "params": params_schema,
    }

    msg = MessageSchema(**message)

    json_message = msg.to_json()

    return json_message


def create_frame_message(bucket, key):
    return_file_options = ReturnFileOptions(
        key_prefix="frame", bucket=DEFAULT_BUCKET
    )

    video_file = S3File(bucket=bucket, key=key)

    frame_params = FrameParams(
        time=1.423219,
        video_file=video_file,
        output_options=return_file_options
    )

    params_schema = ParamsSchema(
        job=JobType.FRAME, job_params=frame_params
    )

    status = {
        "status": MessageStatus.INIT,
        "created_at": datetime.now(),
        "created_by": f"{SERVICE_NAME}-{WORKER_ID}",
    }
    status_schema = StatusSchema(**status)

    message = {
        "id": str(uuid4()),
        "service": SERVICE_NAME,
        "status": [status_schema],
        "params": params_schema,
    }

    msg = MessageSchema(**message)

    json_message = msg.to_json()

    return json_message


def create_hls_message(bucket, key):
    return_file_options = ReturnFileOptions(
        key_prefix="hlsclips", bucket=DEFAULT_BUCKET
    )

    video_options = VideoOptions(
        video_size=None,
        duration=None,
        fps=30,
        codec="libx264",
        codec_extras="veryfast",
    )

    video_file = S3File(bucket=bucket, key=key)

    hls_params = HLSParams(
        ts_duration=3.0,
        crf=23,
        video_file=video_file,
        video_options=video_options,
        output_options=return_file_options,
    )

    params_schema = ParamsSchema(job=JobType.HLS, job_params=hls_params)

    status = {
        "status": MessageStatus.INIT,
        "created_at": datetime.now(),
        "created_by": f"{SERVICE_NAME}-{WORKER_ID}",
    }
    status_schema = StatusSchema(**status)

    message = {
        "id": str(uuid4()),
        "service": SERVICE_NAME,
        "status": [status_schema],
        "params": params_schema,
    }

    msg = MessageSchema(**message)

    json_message = msg.to_json()

    return json_message


def create_tts_message():
    return_file_options = ReturnFileOptions(
        key_prefix="ttsclips", bucket=DEFAULT_BUCKET
    )

    tts_params = TTSParams(
        language="tr-TR",
        voice="tr-TR-Wavenet-A",
        pitch=-2.40,
        speed=1.19,
        text="Merhaba Mustafa, size özel bir haberimiz var.",
        output_options=return_file_options,
    )

    params_schema = ParamsSchema(job=JobType.TTS, job_params=tts_params)

    status = {
        "status": MessageStatus.INIT,
        "created_at": datetime.now(),
        "created_by": f"{SERVICE_NAME}-{WORKER_ID}",
    }
    status_schema = StatusSchema(**status)

    message = {
        "id": str(uuid4()),
        "service": SERVICE_NAME,
        "status": [status_schema],
        "params": params_schema,
    }

    msg = MessageSchema(**message)

    json_message = msg.to_json()

    return json_message


def create_pvi_message(bucket, key):
    _id = str(uuid4())
    return_file_options = ReturnFileOptions(
        key_prefix="pvi_clips", bucket="mustapv"
    )
    m3u8_file = S3File(bucket=bucket, key=key)
    ts_1 = CustomTs(name="deniz-s00.ts",
                    ts_file=S3File("mustapv", "deniz-s00.ts"),
                    text="MUSTAFA IS THE MAN", fontsize=33,
                    fontfile=S3File("mustapv", "Phetsarath_OT.ttf"),
                    color=(0, 0, 0), position=(10, 20), t_start=0.0, t_end=3.0,
                    alpha=1.0)
    ts_2 = CustomTs(name="deniz-s03.ts",
                    ts_file=S3File("mustapv", "deniz-s03.ts"),
                    text="MUSTAFA IS THE MANNEST", fontsize=33,
                    fontfile=S3File("mustapv", "Phetsarath_OT.ttf"),
                    color=(0, 0, 0), position=(10, 20), t_start=0.0, t_end=3.0,
                    alpha=0.0)
    custom_ts_list = [ts_1, ts_2]
    pvi_params = PviParams(m3u8_file=m3u8_file,
                           custom_ts_list=custom_ts_list,
                           output_options=return_file_options,
                           custom_link=_id + ".m3u8")

    params_schema = ParamsSchema(
        job=JobType.PVI, job_params=pvi_params
    )

    status = {
        "status": MessageStatus.INIT,
        "created_at": datetime.now(),
        "created_by": f"{SERVICE_NAME}-{WORKER_ID}",
    }
    status_schema = StatusSchema(**status)

    message = {
        "id": _id,
        "service": SERVICE_NAME,
        "status": [status_schema],
        "params": params_schema,
    }

    msg = MessageSchema(**message)

    json_message = msg.to_json()

    return json_message


def create_message():
    messages = []

    test_color_clip = False
    test_image_clip = False
    test_imageoverlay_clip = False
    test_encode_clip = False
    test_textoverlay_clip = False
    test_concat_clip = False
    test_subclip = False
    test_hls = False
    test_sound = False
    test_frame = False
    test_tts = False
    test_pvi = True

    if test_color_clip:
        color_clip_count = 1
        for i in range(color_clip_count):
            messages.append(create_colorclip_message())

    if test_encode_clip:
        encode_clips = [
            {"bucket": DEFAULT_BUCKET, "key": "Transition05.mov"}
        ]
        for encode_job in encode_clips:
            messages.append(create_encodeclip_message(**encode_job))

    if test_image_clip:
        image_clips = [{"bucket": DEFAULT_BUCKET, "key": "test_img.jpeg"}]
        for encode_job in image_clips:
            messages.append(create_imageclip_message(**encode_job))

    if test_imageoverlay_clip:
        image_clips = [
            {
                "bucket": DEFAULT_BUCKET,
                "key": "test_img.jpeg",
                "overlay_key": "life_at_google.mp4",
            }
        ]
        for encode_job in image_clips:
            messages.append(create_imageclip_message(**encode_job))

    if test_textoverlay_clip:
        image_clips = [{"bucket": DEFAULT_BUCKET, "key": "life_at_google.mp4"}]
        for encode_job in image_clips:
            messages.append(create_textoverlayclip_message(**encode_job))

    if test_concat_clip:
        concat_clips = [
            {"bucket": DEFAULT_BUCKET, "key": "deniz.mp4"},
            {"bucket": DEFAULT_BUCKET, "key": "trans.mov"},
            {"bucket": DEFAULT_BUCKET, "key": "life_at_google.mp4"},
            {"bucket": DEFAULT_BUCKET, "key": "vegas_gb-s14.ts"},
            {"bucket": DEFAULT_BUCKET, "key": "vegas_gb-s15.ts"},
            {"bucket": DEFAULT_BUCKET, "key": "vegas_gb-s16.ts"},
            {"bucket": DEFAULT_BUCKET, "key": "vegas_gb-s17.ts"},
        ]

        messages.append(create_concatclip_message(concat_clips))

    if test_subclip:
        subclips = [{"bucket": DEFAULT_BUCKET, "key": "life_at_google.mp4"}]
        for subclip_job in subclips:
            messages.append(create_subclip_message(**subclip_job))

    if test_hls:
        hls_clips = [{"bucket": DEFAULT_BUCKET, "key": "life_at_google.mp4"}]
        for hls_job in hls_clips:
            messages.append(create_hls_message(**hls_job))

    if test_sound:
        sound_clips = [
            {
                "bucket": DEFAULT_BUCKET,
                "key": "color/bf6bcd12-1d2a-43f9-b67a-b15f9b769967.mp4",
            }
        ]
        for sound_job in sound_clips:
            messages.append(create_soundclip_message(**sound_job))

    if test_frame:
        frame_clips = [{"bucket": DEFAULT_BUCKET, "key": "life_at_google.mp4"}]
        for frame_job in frame_clips:
            messages.append(create_frame_message(**frame_job))

    if test_tts:
        messages.append(create_tts_message())

    if test_pvi:
        messages.append(create_pvi_message("mustapv", "deniz.m3u8"))

    return messages


def write_message():
    r = get_connection()
    try:
        print("Creating consumer group")
        print(r)
        r.xgroup_create(JOBS, REDIS_JOBS_CONSUMER_GROUP, mkstream=True)
        print("Consumer group created")
    except Exception:
        print("Consumer group already exists, continuing")

    for message in create_message():
        key = f"{SERVICE_NAME}-{str(uuid4())}"
        val = {"key": key, "val": message, "conn": r, "stream": JOBS}
        write_to_stream(**val)
        print(f"key: {key}")
        print(f"message: {message}")
        print("")


write_message()
