"""Short summary.

Notes
-----
    Short summary.

"""

from enum import Enum
from datetime import datetime
from dataclasses import dataclass, field
from typing import List, Any

import orjson as json

from libs.models import MessageSchema


class JobType(Enum):
    COLOR = 1
    TEXT = 2
    IMAGE = 3
    TS = 4
    ENCODE = 5
    META = 6
    CONCAT = 7
    SUBCLIP = 8
    HLS = 9
    SOUNDMIX = 10
    FRAME = 11
    TTS = 12
    PVI = 13


@dataclass
class S3File:
    """Short summary.

    Parameters
    ----------
    bucket : str
        Description of parameter `bucket`.
    key : str
        Description of parameter `key`.

    """

    bucket: str = field(default=None)
    key: str = field(default=None)


@dataclass
class ReturnFileOptions:
    """Short summary.

    Parameters
    ----------
    bucket : str
        Description of parameter `bucket`.
    key_prefix : str
        Description of parameter `key_prefix`.

    """

    key_prefix: str
    bucket: str = field(default=None)


@dataclass
class OverlayOptions:
    """Short summary.

    Parameters
    ----------
    video_file : S3File
        Base video for overlaying.  This video will be used as background.
    t_start: float
        Overlay start time. If None, it will be shown on whole duration
        of the base video.
    t_end: float
        Overlay end time. If None, it will be shown on whole duration
        of the base video.
    position : tuple
        Pixels. Position of the overlay to the top left corner
        of the base video.
    size: tuple
        Width and height of the overlay. Pixels. (100, 120) for a 100x120
        overlay.
    """

    video_file: S3File
    t_start: float = field(default=None)
    t_end: float = field(default=None)
    position: tuple = field(default=(0, 0))
    size: tuple = field(default=None)


@dataclass
class VideoOptions:
    """Short summary.

    Parameters
    ----------
    video_size : Tuple
        Set to (desired_height, desired_width) to have ffmpeg resize
        the frames before returning them. This is much faster than streaming
        in high-res and then resizing. If either dimension is None, the frames
        are resized by keeping the existing aspect ratio.

        If set to 'None' in encoding and overlay operations, the original video
        size is preserved
    duration : Float
        Description of parameter `duration`.
    fps : float
        Description of parameter `fps`.
    codec : float
        Description of parameter `codec`.
    codec_extras : str
        This option is used for 'preset' value of FFMPEG.
        Sets the time that FFMPEG will spend optimizing the compression.
        Choices are: ultrafast, superfast, veryfast, faster, fast, medium,
        slow, slower, veryslow, placebo.
        Note that this does not impact the quality of the video, only the
        size of the video file. So choose ultrafast when you are in a hurry
        and file size does not matter.

    """

    video_size: tuple = field(default=None)
    duration: float = field(default=None)
    fps: float = field(default=None)
    codec: str = field(default=None)
    codec_extras: str = field(default=None)


@dataclass
class EncodeParams:
    """Short summary.

    Parameters
    ----------
    video_file : S3File
        Description of parameter `bucket`.
    output_options : ReturnFileOptions
        Description of parameter `key_prefix`.
    video_options : VideoOptions
        Description of parameter `video_options`.

    """

    video_file: S3File
    output_options: ReturnFileOptions = field(default=None)
    video_options: VideoOptions = field(default=None)

    def convert_self(self):
        self.video_file = S3File(**self.video_file)
        self.output_options = ReturnFileOptions(**self.output_options)
        self.video_options = VideoOptions(**self.video_options)


@dataclass
class ColorClipParams:
    """Short summary.

    Parameters
    ----------
    color : tuple
        Description of parameter `color`.
    video_options : VideoOptions
        Description of parameter `video_options`.
    output_options : ReturnFileOptions
        Description of parameter `output_options`.

    """

    color: Any
    video_options: VideoOptions = field(default=None)
    output_options: ReturnFileOptions = field(default=None)

    def convert_self(self):
        self.video_options = VideoOptions(**self.video_options)
        self.output_options = ReturnFileOptions(**self.output_options)


@dataclass
class ImageClipParams:
    """Short summary.

    Parameters
    ----------
    video_file : S3File
        Description of parameter `video_file`.
    output_options : ReturnFileOptions
        Description of parameter `output_options`.
    overlay_options : OverlayOptions
        Description of parameter `overlay_options`.

    """

    image_file: S3File
    video_options: VideoOptions = field(default=None)
    output_options: ReturnFileOptions = field(default=None)
    overlay_options: OverlayOptions = field(default=None)

    def convert_self(self):
        self.image_file = S3File(**self.image_file)
        self.video_options = VideoOptions(**self.video_options)
        self.output_options = ReturnFileOptions(**self.output_options)

        if self.overlay_options:
            overlay_options = OverlayOptions(**self.overlay_options)
            overlay_options.video_file = S3File(**overlay_options.video_file)
            self.overlay_options = overlay_options


@dataclass
class TextClipParams:
    text: str
    color: Any
    font: Any
    alpha: float = field(default=1.0)
    text_size: int = field(default=25)
    method: str = field(default="caption")
    video_options: VideoOptions = field(default=None)
    output_options: ReturnFileOptions = field(default=None)
    overlay_options: OverlayOptions = field(default=None)

    def convert_self(self):
        self.video_options = VideoOptions(**self.video_options)
        self.output_options = ReturnFileOptions(**self.output_options)

        if self.overlay_options:
            overlay_options = OverlayOptions(**self.overlay_options)
            overlay_options.video_file = S3File(**overlay_options.video_file)
            self.overlay_options = overlay_options

        if self.font and isinstance(self.font, dict):
            self.font = S3File(**self.font)


@dataclass
class ConcatParams:
    video_files: List[S3File] = field(default_factory=list)
    video_options: VideoOptions = field(default=None)
    output_options: ReturnFileOptions = field(default=None)

    def convert_self(self):
        self.video_options = VideoOptions(**self.video_options)
        self.output_options = ReturnFileOptions(**self.output_options)
        tmp_list = [S3File(**i) for i in self.video_files]
        self.video_files = tmp_list


@dataclass
class SubClipParams:
    """Short summary.

    Parameters
    ----------
    video_file : S3File
        Description of parameter `bucket`.
    output_options : ReturnFileOptions
        Description of parameter `key_prefix`.
    video_options : VideoOptions
        Description of parameter `video_options`.

    """

    start: float
    end: float
    video_file: S3File
    output_options: ReturnFileOptions = field(default=None)
    video_options: VideoOptions = field(default=None)
    keyframe: bool = field(default=False)

    def convert_self(self):
        self.video_file = S3File(**self.video_file)
        self.video_options = VideoOptions(**self.video_options)
        self.output_options = ReturnFileOptions(**self.output_options)


@dataclass
class HLSParams:
    """Short summary.

    Parameters
    ----------
    video_file : S3File
        Description of parameter `bucket`.
    output_options : ReturnFileOptions
        Description of parameter `key_prefix`.
    video_options : VideoOptions
        Description of parameter `video_options`.

    """

    ts_duration: float
    crf: int
    video_file: S3File
    output_options: ReturnFileOptions = field(default=None)
    video_options: VideoOptions = field(default=None)

    def convert_self(self):
        self.video_file = S3File(**self.video_file)
        self.output_options = ReturnFileOptions(**self.output_options)
        self.video_options = VideoOptions(**self.video_options)


@dataclass
class SoundMixClipParams:
    """Short summary.

    Parameters
    ----------
    sound_file : S3File
        Description of parameter `sound_file`.
    sound_start : float
        The time that the sound should start. In seconds eg. 10.45
    video_options : VideoOptions
        Description of parameter `video_options`.
    output_options : ReturnFileOptions
        Description of parameter `output_options`.
    overlay_options : OverlayOptions
        Description of parameter `overlay_options`.

    """

    sound_file: S3File
    sound_start: float = field(default=None)
    video_options: VideoOptions = field(default=None)
    output_options: ReturnFileOptions = field(default=None)
    overlay_options: OverlayOptions = field(default=None)

    def convert_self(self):
        self.video_options = VideoOptions(**self.video_options)
        self.output_options = ReturnFileOptions(**self.output_options)
        self.sound_file = S3File(**self.sound_file)

        if self.overlay_options:
            overlay_options = OverlayOptions(**self.overlay_options)
            overlay_options.video_file = S3File(**overlay_options.video_file)
            self.overlay_options = overlay_options


@dataclass
class FrameParams:
    """Short summary.

    Parameters
    ----------
    video_file : S3File
        Video File from the frame image will be extracted.
    time: Float
        Exact time of the video frame in seconds. eg: 10.2354
    output_options : ReturnFileOptions
        Description of parameter `key_prefix`.

    """

    video_file: S3File
    time: float
    output_options: ReturnFileOptions = field(default=None)

    def convert_self(self):
        self.video_file = S3File(**self.video_file)
        self.output_options = ReturnFileOptions(**self.output_options)


@dataclass
class TTSParams:
    """Short summary.

    Parameters
    ----------
    language: str
        Speech language. Must be tr-TR or en-US.
    voice: str
        Voice type. Eg: tr-TR-Wavenet-B
    pitch: float
        Voice pitch.
    speed: float
        Voice speed.
    text: str
        Text which will be converted to speech.
    output_options : ReturnFileOptions
        Description of parameter `key_prefix`.
    """

    language: str
    voice: str
    pitch: float
    speed: float
    text: str
    output_options: ReturnFileOptions = field(default=None)

    def convert_self(self):
        self.output_options = ReturnFileOptions(**self.output_options)


@dataclass
class CustomTs:
    name: str
    ts_file: S3File
    text: str
    fontsize: int
    # Having a font file is the better practice.
    fontfile: S3File
    color: tuple = field(default=(0, 0, 0))
    position: tuple = field(default=(0, 0))
    t_start: float = field(default=0.0)
    t_end: float = field(default=0.0)
    alpha: float = field(default=0.0)

    def convert_self(self):
        self.ts_file = S3File(**self.ts_file)


@dataclass
class PviParams:
    m3u8_file: S3File
    custom_ts_list: List[CustomTs] = field(default_factory=list)
    output_options: ReturnFileOptions = field(default=None)
    custom_link: str = field(default=None)
    def convert_self(self):
        self.output_options = ReturnFileOptions(**self.output_options)
        self.m3u8_file = S3File(**self.m3u8_file)
        self.custom_ts_list = [CustomTs(**i) for i in self.custom_ts_list]

@dataclass
class ParamsSchema:
    """Short summary.

    Parameters
    ----------
    job : JobType
        Description of parameter `bucket`.
    job_params : Any
        Possible options are EncodeParams, ColorClipParams

    """

    job: JobType
    job_params: Any


@dataclass
class MetaResponseSchema:
    video_fps: float
    duration: float
    width: int
    height: int
    video_frames: int
    audio_exists: bool = field(default=False)
    audio_frames: int = field(default=None)
    audio_fps: float = field(default=None)


@dataclass
class ResponseSchema:
    video_file: S3File
    created_by: str
    metadata: MetaResponseSchema = field(default=None)
    created_at: datetime = field(default=datetime.now())


@dataclass
class VidToolsMessageScheme(MessageSchema):
    params: ParamsSchema = field(default_factory=ParamsSchema)
    response: List[ResponseSchema] = field(default_factory=list)

    def set_response(self, response_scheme):
        """Short summary.

        Parameters
        ----------
        response_scheme : ResponseSchema
            Description of parameter `response_scheme`.

        Returns
        -------
        None
            Description of returned object.

        """
        self.response.append(response_scheme)

    def convert_params(self):
        self.params = ParamsSchema(**self.params)
        self.params.job = JobType(self.params.job)

        if self.params.job == JobType.COLOR:
            self.params.job_params = ColorClipParams(**self.params.job_params)

        if self.params.job == JobType.IMAGE:
            self.params.job_params = ImageClipParams(**self.params.job_params)

        if self.params.job == JobType.ENCODE:
            self.params.job_params = EncodeParams(**self.params.job_params)

        if self.params.job == JobType.TEXT:
            self.params.job_params = TextClipParams(**self.params.job_params)

        if self.params.job == JobType.CONCAT:
            self.params.job_params = ConcatParams(**self.params.job_params)

        if self.params.job == JobType.SUBCLIP:
            self.params.job_params = SubClipParams(**self.params.job_params)

        if self.params.job == JobType.HLS:
            self.params.job_params = HLSParams(**self.params.job_params)

        if self.params.job == JobType.SOUNDMIX:
            self.params.job_params = SoundMixClipParams(
                **self.params.job_params
            )

        if self.params.job == JobType.FRAME:
            self.params.job_params = FrameParams(**self.params.job_params)

        if self.params.job == JobType.TTS:
            self.params.job_params = TTSParams(**self.params.job_params)

        if self.params.job == JobType.PVI:
            self.params.job_params = PviParams(**self.params.job_params)

        self.params.job_params.convert_self()

    def convert_response(self):
        # TODO: This needs to be implemented
        ...
