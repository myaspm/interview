"""Short summary.

Notes
-----
    Short summary.

"""
import subprocess
import os.path
from collections.abc import Iterable
from uuid import uuid4

from conf.log import log
from conf.config import TEMP_DIR
from service.libs.models import S3File
from service.libs.file_operations import download_file, remove_tmp_file

import ffmpeg
from Katna.image import Image
from google.cloud import texttospeech

DEFAULT_CODEC = "libx264"


def get_clip_meta(local_file_path=None):
    """Short summary.

    Parameters
    ----------
    local_file_path : str
        Description of parameter `local_file_path`.
    video_obj : Clip
        Description of parameter `video_obj`.

    Returns
    -------
    type
        Description of returned object.

    """

    meta_dict = {}

    if local_file_path:
        try:
            # Select audio stream
            meta_dict["audio_exists"] = False
            meta_dict["audio_frames"] = 0
            meta_dict["audio_fps"] = 0.0
            audio_probe = ffmpeg.probe(local_file_path, select_streams="a")[
                "streams"
            ]

            if len(audio_probe) == 1:
                meta_dict["audio_exists"] = True
                # TS files don't have frames data.
                try:
                    meta_dict["audio_frames"] = int(
                        audio_probe[0]["nb_frames"]
                    )
                except Exception:
                    meta_dict["audio_frames"] = 0

                audio_fps_0 = float(
                    audio_probe[0]["r_frame_rate"].split("/")[0]
                )
                audio_fps_1 = float(
                    audio_probe[0]["r_frame_rate"].split("/")[1]
                )
                if audio_fps_1 != 0:
                    # Division by zero.....
                    meta_dict["audio_fps"] = audio_fps_0 / audio_fps_1

            # Select video stream
            video_probe = ffmpeg.probe(local_file_path, select_streams="v")[
                "streams"
            ]

            if len(video_probe) == 1:
                fps_0 = float(video_probe[0]["r_frame_rate"].split("/")[0])
                fps_1 = float(video_probe[0]["r_frame_rate"].split("/")[1])
                meta_dict["video_fps"] = 0.0
                if fps_1 != 0:
                    # Division by zero.....
                    meta_dict["video_fps"] = fps_0 / fps_1

                meta_dict["duration"] = float(video_probe[0]["duration"])
                meta_dict["width"] = video_probe[0]["width"]
                meta_dict["height"] = video_probe[0]["height"]
                try:
                    meta_dict["video_frames"] = int(
                        video_probe[0]["nb_frames"]
                    )
                except Exception:
                    meta_dict["video_frames"] = 0

            return meta_dict

        except Exception as exc:
            msg = f"Error while extracting metadata from {local_file_path}"
            msg = f"{msg} Reason: {exc}"
            log.error(msg)

    return None


def encode(source_file, destination_file, clip_options):
    """Short summary.

    Parameters
    ----------
    source_file : str
        Description of parameter `source_file`.
    destination_file : EncodeParams
        Description of parameter `destination_file`.
    clip_options : Dict

    Returns
    -------
    type
        Description of returned object.

    """

    try:
        options = clip_options.video_options
        outfile_args = generate_outfile_args(options)

        (
            ffmpeg.input(source_file)
            .output(destination_file, **outfile_args)
            .run()
        )

        msg = f"{source_file} is encoded succesfully. File: {destination_file}"
        log.info(msg)

        return destination_file

    except Exception as exc:
        msg = f"Error while encoding object from {source_file} Reason: {exc}"
        log.error(msg)

    return None


def generate_text_overlay(source_file, destination_file, clip_options):
    """Short summary.

    Parameters
    ----------
    source_file : str
        Description of parameter `source_file`.
    destination_file : str
        Description of parameter `destination_file`.
    clip_options : TextClipParams
        Description of parameter `clip_options`.

    Returns
    -------
    str
        Description of returned object.

    """
    try:
        position_x = clip_options.overlay_options.position[0]
        position_y = clip_options.overlay_options.position[1]

        font_file, font = font_ops(clip_options.font)

        outfile_args = generate_outfile_args(clip_options.video_options)
        vprobe = ffmpeg.probe(source_file, select_streams="v")
        aprobe = ffmpeg.probe(source_file, select_streams="a")

        color = check_and_generate_color(clip_options.color)
        if not color:
            raise Exception("Wrong color type, please refer to docs.")

        file_ext = os.path.splitext(source_file)[1]
        input_stream = ffmpeg.input(source_file)

        t_start, t_end = check_overlay_times(
            clip_options.overlay_options.t_start,
            clip_options.overlay_options.t_end,
            vprobe["streams"][0]["duration"],
        )

        overlay = input_stream.video.drawtext(
            text=clip_options.text,
            fontcolor=color,
            fontsize=clip_options.text_size,
            fontfile=font_file,
            font=font,
            enable=f"between(t,{t_start},{t_end})",
            x=position_x,
            y=position_y,
            alpha=clip_options.alpha,
        )

        overlayed_vid_path = os.path.join(
            TEMP_DIR, f"{str(uuid4())}{file_ext}"
        )

        (ffmpeg.output(overlay, overlayed_vid_path, **outfile_args).run())

        if not len(aprobe["streams"]) == 0:
            overlayed_vid_path = create_overlay_with_audio(
                overlayed_vid_path, file_ext, input_stream.audio
            )

        outfile_args = generate_outfile_args(clip_options.video_options)
        outfile_args["muxdelay"] = 0
        outfile_args["muxpreload"] = 0
        outfile_args["output_ts_offset"] = vprobe["streams"][0]["start_time"]

        (
            ffmpeg.input(overlayed_vid_path)
            .output(destination_file, **outfile_args)
            .run()
        )

        log.info("Text overlay is succesfull. File: %s", destination_file)

        # Remove fonts and intermediate video files.
        if font_file:
            remove_tmp_file(font_file)
        if overlayed_vid_path:
            remove_tmp_file(overlayed_vid_path)

        return destination_file

    except Exception as exc:
        log.error("Error while encoding text overlay Reason: %s", exc)

    return None


def generate_color_clip(destination_file, clip_options):
    """Short summary.

    Parameters
    ----------
    destination_file : str
        Description of parameter `destination_file`.
    clip_options : Dict
        Description of parameter `clip_options`.
    codec : str
        Description of parameter `codec`.

    Returns
    -------
    str
        Description of returned object.

    """
    options = clip_options.video_options
    outfile_args = generate_outfile_args(options)

    color_initial = clip_options.color

    if isinstance(color_initial, Iterable) and len(color_initial) == 3:
        color = rgb2hex(color_initial[0], color_initial[1], color_initial[2])
    elif isinstance(color_initial, str):
        color = color_initial
    else:
        raise Exception("Incorrect color type, please refer to docs.")

    (
        ffmpeg.input(f"color=c={color}", f="lavfi")
        .output(destination_file, **outfile_args)
        .run()
    )

    msg = f"Generating a new {clip_options.color} colored VideoFileClip is succesfull."
    msg = f"{msg} File: {destination_file}"
    log.info(msg)

    return destination_file


def generate_image_clip(image_file, destination_file, clip_options):
    """Short summary.

    Parameters
    ----------
    image_file : str
        Description of parameter `image_file`.
    destination_file : str
        Description of parameter `destination_file`.
    clip_options : Dict
        Description of parameter `clip_options`.

    Returns
    -------
    str
        Description of returned object.

    """
    try:
        infile_args, outfile_args = {}, {}
        options = clip_options.video_options

        width = ffmpeg.probe(image_file)["streams"][0]["width"]
        height = ffmpeg.probe(image_file)["streams"][0]["height"]
        # Ensure width/height is divisible by 2
        if width % 2 != 0:
            width += 1
        if height % 2 != 0:
            height += 1

        # Use picture res only if no video resolution parameters is given.
        # That's why we generate args after giving widthxheight by hand.
        outfile_args["s"] = f"{width}x{height}"
        outfile_args = generate_outfile_args(options)

        infile_args["loop"] = 1

        (
            ffmpeg.input(image_file, **infile_args)
            .output(destination_file, **outfile_args)
            .run()
        )

        msg = "Generating a new image clip is succesfull."
        msg = f"{msg} File: {destination_file}"
        log.info(msg)

        return destination_file

    except Exception as exc:
        msg = f"Error while generating image clip Reason: {exc}"
        log.error(msg)

        return None


def generate_image_overlay(
    source_file, source_image, destination_file, clip_options
):
    """Short summary.

    Parameters
    ----------
    source_file : str
        Description of parameter `source_file`.
    source_image : str
        Description of parameter `source_file`.
    destination_file : str
        Description of parameter `destination_file`.
    clip_options : Dict
        Description of parameter `clip_options`.

    Returns
    -------
    str
        Description of returned object.

    """
    try:

        options = clip_options.video_options
        overlay = clip_options.overlay_options
        vprobe = ffmpeg.probe(source_file, select_streams="v")
        overlay_args = {}

        if overlay.size:
            width = overlay.size[0]
            height = overlay.size[1]
            img_module = Image()
            resized_img = img_module.resize_image(source_image, width, height)
            img_module.save_image_to_disk(
                resized_img,
                os.path.dirname(source_image),
                "resized_img",
                os.path.splitext(source_image)[1],
            )
        overlay_fp = os.path.join(
            os.path.dirname(source_image),
            "resized_img" + os.path.splitext(source_image)[1],
        )

        t_start, t_end = check_overlay_times(
            overlay.t_start, overlay.t_end, vprobe["streams"][0]["duration"]
        )
        outfile_args = generate_outfile_args(options)

        if overlay.position:
            overlay_args["x"] = overlay.position[0]
            overlay_args["y"] = overlay.position[1]

        overlay_args["enable"] = f"between(t,{t_start},{t_end})"

        infile = ffmpeg.input(source_file)
        overlay_file = ffmpeg.input(overlay_fp)
        audio = infile.audio
        video = infile.video
        overlay = video.overlay(overlay_file, **overlay_args)
        cmd = ffmpeg.output(audio, overlay, destination_file, **outfile_args)
        cmd.run()

        msg = "Generating a new image overlay clip is succesfull."
        msg = f"{msg} File: {destination_file}"
        log.info(msg)

        return destination_file, overlay_fp

    except Exception as exc:
        msg = f"Error while generating image clip Reason: {exc}"
        log.error(msg)

        return None


def concat_videos(file_list, destination_file, clip_options):
    try:
        outfile_args = generate_outfile_args(clip_options.video_options)
        outfile_args["max_muxing_queue_size"] = 9999
        # TS Spesific
        if all(".ts" in i for i in file_list):
            if not outfile_args.get("vcodec", None):
                # Codec of the first file
                vcodec = ffmpeg.probe(
                    file_list[0],
                    select_streams="v:0",
                    show_entries="stream=codec_name",
                )["streams"][0]["codec_name"]
                outfile_args["c:v"] = vcodec

        # All other conditions
        else:
            # Create intermediate .TS files.
            # outfile_args["bsf:v"] = "h264_mp4toannexb"
            inter_file_list = []
            for vid_file in file_list:
                if ".ts" not in vid_file:
                    tmp_file = os.path.join(TEMP_DIR, f"{str(uuid4())}.ts")
                    (
                        ffmpeg.input(vid_file)
                        .output(tmp_file, **outfile_args, f="mpegts")
                        .run()
                    )
                    inter_file_list.append(tmp_file)
                else:
                    inter_file_list.append(vid_file)

            file_list = inter_file_list

        in_str = f"concat:{'|'.join(file_list)}"
        (ffmpeg.input(in_str).output(destination_file, **outfile_args).run())
        # Remove intermediate files
        [remove_tmp_file(i) for i in file_list]

        return destination_file

    except Exception as exc:
        msg = f"Error while creating concat vid. Reason: {exc}"
        log.error(msg)

    return None


def create_subclip(source_file, destination_file, clip_options):
    try:
        options = clip_options.video_options

        outfile_args = generate_outfile_args(options)

        infile_args = {}
        infile_args["ss"] = clip_options.start
        infile_args["to"] = clip_options.end

        if clip_options.keyframe:
            interval = f"{clip_options.start}%{clip_options.end}"
            frames = ffmpeg.probe(
                source_file,
                read_intervals=interval,
                select_streams="v",
                skip_frame="nokey",
                show_entries="frame=pkt_pts_time",
            )["frames"]
            infile_args["ss"] = float(frames[0]["pkt_pts_time"])
            infile_args["to"] = float(frames[-1]["pkt_pts_time"])

        (
            ffmpeg.input(source_file, **infile_args)
            .output(destination_file, **outfile_args)
            .run()
        )

        msg = f"{source_file} is clipped succesfully. File: {destination_file}"
        log.info(msg)

        return destination_file

    except Exception as exc:
        msg = f"Error while creating subclip from {source_file} Reason: {exc}"
        log.error(msg)

    return None


def create_hls_from_vid(source_file, destination_file, clip_options):
    try:
        options = clip_options.video_options
        outfile_args = generate_outfile_args(options)
        dest_file_without_ext = destination_file.replace(".m3u8", "")
        outfile_args["crf"] = clip_options.crf
        outfile_args["keyint_min"] = clip_options.video_options.fps
        outfile_args["g"] = clip_options.video_options.fps
        outfile_args["c:a"] = "aac"
        outfile_args["profile:v"] = "main"
        outfile_args["b:a"] = "192k"
        outfile_args["ar"] = 48000
        outfile_args["sc_threshold"] = 0
        outfile_args["hls_list_size"] = 0
        outfile_args["hls_time"] = clip_options.ts_duration
        outfile_args["hls_playlist_type"] = "vod"
        outfile_args[
            "hls_segment_filename"
        ] = f"{dest_file_without_ext}-s%02d.ts"

        (
            ffmpeg.input(source_file)
            .output(destination_file, **outfile_args)
            .run()
        )

        msg = (
            f"{source_file} is converted to HLS Stream successfully. "
            f"File: {destination_file}"
        )
        log.info(msg)

        ts_files = get_ts_files_list(destination_file)

        return destination_file, ts_files

    except Exception as exc:
        msg = f"Error while creating subclip from {source_file} Reason: {exc}"
        log.error(msg)

    return None


def create_tts_file(destination_file, options):
    try:
        client = texttospeech.TextToSpeechClient()
        synthesis_input = texttospeech.SynthesisInput(text=options.text)

        voice = texttospeech.VoiceSelectionParams(
            language_code=options.language, name=options.voice
        )

        audio_config = texttospeech.AudioConfig(
            audio_encoding=texttospeech.AudioEncoding.MP3,
            pitch=options.pitch,
            speaking_rate=options.speed,
        )

        response = client.synthesize_speech(
            input=synthesis_input, voice=voice, audio_config=audio_config
        )
        with open(destination_file, "wb") as sound_file:
            sound_file.write(response.audio_content)

        return destination_file

    except Exception as exc:
        msg = f"Error while generating text to speech. Reason: {exc}"
        log.error(msg)

    return None


def generate_sound_mix_clip(
    source_file, sound_file, destination_file, clip_options
):
    try:
        options = clip_options.video_options

        outfile_args = generate_outfile_args(options, complex_filter=True)
        source_vprobe = ffmpeg.probe(source_file, select_streams="v")
        source_aprobe = ffmpeg.probe(source_file, select_streams="a")
        sound_aprobe = ffmpeg.probe(sound_file, select_streams="a")

        sound_file = ffmpeg.input(sound_file)
        video_file = ffmpeg.input(source_file)

        # t_end won't be used because ffmpeg only accepts delay start time.
        t_start, dummy_t_end = check_overlay_times(
            clip_options.sound_start,
            None,
            source_vprobe["streams"][0]["duration"],
        )

        outfile_args["c:v"] = "copy"
        # FFMPEG wants delay time to be in miliseconds.
        t_start = 1000 * t_start

        # Build mix cmd.
        # Check if source video has audio.
        if len(source_aprobe["streams"]) == 0:
            filter_cmd = f"[0:a]adelay={t_start}|{t_start}"
        else:
            filter_cmd = (
                f"[0:a]adelay={t_start}|{t_start}[aud0];"
                f"[1:a][aud0]amix=inputs=2"
            )

        outfile_args["filter_complex"] = filter_cmd

        # Check if the sound is longer than the video.
        if (
            sound_aprobe["streams"][0]["duration"]
            > source_vprobe["streams"][0]["duration"]
        ):
            outfile_args["shortest"] = None

        cmd = ffmpeg.output(
            sound_file, video_file, destination_file, **outfile_args
        )
        cmd.run()

        return destination_file

    except Exception as exc:
        msg = f"Error while mixing sound. Reason: {exc}"
        log.error(msg)

    return None


def generate_frame_from_video(source_file, destination_file, clip_options):
    try:
        # ss -> Seek Start
        # -frames:v parameter must be given in order to extract a frame.
        outfile_args = {"frames:v": 1}
        (
            ffmpeg.input(source_file, ss=clip_options.time)
            .output(destination_file, **outfile_args)
            .run()
        )

        msg = (
            f"Frame at {clip_options.time} is extracted from {source_file}."
            f"File: {destination_file}"
        )
        log.info(msg)
        return destination_file

    except Exception as exc:
        msg = f"Error while extracting frame. Reason: {exc}"
        log.error(msg)

    return None


def create_pvi_clips(source_file, destination_file, clip_options):
    upload_dict = {}
    out_path_key = destination_file.split('/')[1].replace('.m3u8', '')
    out_m3u8 = open(source_file, "r").read()
    # Download TS files and get paths:
    for i in clip_options.custom_ts_list:
        source_ts_file = download_file(i.ts_file["key"], i.ts_file["bucket"])
        font_file = download_file(i.fontfile["key"], i.fontfile["bucket"])
        input_stream = ffmpeg.input(source_ts_file)
        vprobe = ffmpeg.probe(source_ts_file, select_streams="v")
        aprobe = ffmpeg.probe(source_ts_file, select_streams="a")

        color = check_and_generate_color(i.color)
        if not color:
            raise Exception("Wrong color type, please refer to docs.")

        t_start, t_end = check_overlay_times(
            i.t_start,
            i.t_end,
            vprobe["streams"][0]["duration"],
        )

        overlay = input_stream.video.drawtext(
            text=i.text,
            fontcolor=color,
            fontsize=i.fontsize,
            fontfile=font_file,
            enable=f"between(t,{t_start},{t_end})",
            x=i.position[0],
            y=i.position[1],
            alpha=i.alpha,
        )

        outfile_args = {}

        outfile_args["c:v"] = "libx264"
        outfile_args["c:a"] = "aac"

        outfile_args["preset"] = "veryslow"

        overlayed_vid_path = os.path.join(TEMP_DIR, f"{str(uuid4())}.ts")

        (
            ffmpeg.output(overlay, overlayed_vid_path, **outfile_args).run(
                overwrite_output=True
            )
        )

        if not len(aprobe["streams"]) == 0:
            overlayed_vid_path = create_overlay_with_audio(
                overlayed_vid_path, ".ts", input_stream.audio
            )

        outfile_args = {}
        outfile_args["muxdelay"] = 0
        outfile_args["muxpreload"] = 0
        outfile_args["output_ts_offset"] = vprobe["streams"][0]["start_time"]
        outfile_args["c:v"] = "libx264"
        outfile_args["c:a"] = "aac"
        out_name = f"{i.name.split('.ts')[0]}_custom.ts"
        (
            ffmpeg.input(overlayed_vid_path)
            .output(TEMP_DIR + "/" + out_name, **outfile_args)
            .run(overwrite_output=True)
        )
        upload_dict[f"{out_path_key}/{out_name}"] = TEMP_DIR + "/" + out_name
        out_m3u8 = out_m3u8.replace(i.name, f"{out_path_key}/{out_name}")

    with open(destination_file, "w") as dest:
        dest.write(out_m3u8)
    upload_dict[f"{out_path_key}.m3u8"] = destination_file

    # Clear intermediate files.
    #clear_tmp_dir()

    return upload_dict


def generate_outfile_args(options, complex_filter=False):
    outfile_args = {}

    # Shouldn't be used in complex filters.
    if not complex_filter:
        outfile_args["c:v"] = "copy"
        outfile_args["c:a"] = "copy"

    # This could be changed to veryslow in PROD maybe?
    outfile_args["preset"] = "veryfast"

    if options.video_size:
        outfile_args["s"] = f"{options.video_size[0]}x{options.video_size[1]}"
    if options.codec_extras:
        outfile_args["preset"] = options.codec_extras
    if options.fps:
        outfile_args["r"] = options.fps
    if options.codec:
        outfile_args["c:v"] = options.codec
    if options.duration:
        outfile_args["t"] = options.duration

    return outfile_args


def rgb2hex(red, green, blue):
    return f"#{red:0>2X}{green:0>2X}{blue:0>2X}"


def check_and_generate_color(color_initial):
    if isinstance(color_initial, Iterable) and len(color_initial) == 3:
        return rgb2hex(color_initial[0], color_initial[1], color_initial[2])
    if isinstance(color_initial, str):
        return color_initial
    return None


def font_ops(font=None):
    cmd = ["fc-list"]
    with subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    ) as pipe:
        out, dummy_err = pipe.communicate()
        font_list = [
            i.split(":")[1][1:] for i in out.decode().split("\n")[:-1]
        ]
        if isinstance(font, str):
            if font not in font_list:
                raise Exception("Font not found in system!")
            return None, font
        if isinstance(font, S3File):
            return download_file(font.key, font.bucket), None

        return None, "Times New Roman"


def get_ts_files_list(m3u8_file):
    path_list = []
    with open(m3u8_file, "r", encoding="utf8") as file_pointer:
        # "LAZY FIX FOR NOW"
        folder = None
        if len(m3u8_file.split("/")) > 1:
            folder = m3u8_file.split("/")[0]
        for i in file_pointer:
            if "EXT" not in i:
                if folder:
                    path = f"{folder}/{i}".rstrip()
                else:
                    path = i.rstrip()
                key = i.rstrip()
                path_list.append((path, key))

    return path_list


def check_overlay_times(t_start, t_end, duration):
    if not t_start:
        t_start = 0.0
    if not t_end:
        t_end = duration

    return t_start, t_end


def create_overlay_with_audio(overlayed_silent_vid_path, file_ext, audio):
    overlayed_silent_vid = ffmpeg.input(overlayed_silent_vid_path)
    merged_vid_path = os.path.join(TEMP_DIR, f"{str(uuid4())}{file_ext}")
    # Don't let ffmpeg select codecs by givin c:a and c:v params.
    cmd_merged = ffmpeg.output(
        audio,
        overlayed_silent_vid,
        merged_vid_path,
        **{"c:a": "copy", "c:v": "copy"},
    )
    cmd_merged.run()
    return merged_vid_path


def clear_tmp_dir():
    [remove_tmp_file(os.path.join(TEMP_DIR, i)) for i in os.listdir(TEMP_DIR)]
