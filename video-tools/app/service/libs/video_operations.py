import os.path

from conf.log import log
from conf.config import DEFAULT_BUCKET, TEMP_DIR

from service.libs.file_operations import download_file, remove_tmp_file
from service.libs.utils import (
    generate_random_filename,
    upload_and_remove,
    get_meta_response_scheme,
    get_response_scheme,
)
from service.libs.vid_utils import (
    generate_color_clip,
    generate_image_clip,
    generate_image_overlay,
    generate_text_overlay,
    get_clip_meta,
    encode,
    concat_videos,
    create_subclip,
    create_hls_from_vid,
    generate_sound_mix_clip,
    generate_frame_from_video,
    create_tts_file,
    create_pvi_clips,
)


def image_clip(options):
    """Short summary.

    Parameters
    ----------
    options : ImageClipParams
        Description of parameter `options`.

    Returns
    -------
    ResponseSchema
        Description of returned object.

    """
    # ReturnFileOptions

    key_prefix = "image_clips"
    bucket = DEFAULT_BUCKET

    if options.output_options:
        key_prefix = options.output_options.key_prefix
        if options.output_options.bucket:
            bucket = options.output_options.bucket

    try:
        # Check if overlay op or generate img op, create key accordingly
        ext = "mp4"
        if options.overlay_options:
            ext = os.path.splitext(options.overlay_options.video_file.key)[1][
                1:
            ]

        key, destination_file = generate_random_filename(key_prefix, ext=ext)
        image_file = download_file(
            options.image_file.key, options.image_file.bucket
        )

        overlay_options = options.overlay_options

        if overlay_options:
            source_file = download_file(
                overlay_options.video_file.key,
                overlay_options.video_file.bucket,
            )
            destination_file, overlay_file = generate_image_overlay(
                source_file, image_file, destination_file, options
            )
        else:
            destination_file = generate_image_clip(
                image_file, destination_file, options
            )
        if destination_file:
            msg = "Generating a new Image Clip is succesfull."
            msg = f"{msg} File: {destination_file}"
            log.info(msg)
            meta_dict = get_clip_meta(local_file_path=destination_file)

            remove_tmp_file(image_file)
            if overlay_options:
                remove_tmp_file(source_file)
                remove_tmp_file(overlay_file)

            meta_reponse = get_meta_response_scheme(meta_dict)

            file_operations = upload_and_remove(destination_file, key, bucket)

            if not file_operations:
                raise Exception("Image Clip Operation Error")

            response_scheme = get_response_scheme(meta_reponse, key)
            return response_scheme

    except Exception as exc:
        msg = f"Error while generating Image Clip Reason: {exc}"
        log.error(msg)

    return None


def text_overlay_clip(options):
    """Short summary.

    Parameters
    ----------
    options : TextClipParams
        Description of parameter `options`.

    Returns
    -------
    ResponseSchema
        Description of returned object.

    """
    # ReturnFileOptions

    key_prefix = "color_clips"
    bucket = DEFAULT_BUCKET

    if options.output_options:
        key_prefix = options.output_options.key_prefix
        if options.output_options.bucket:
            bucket = options.output_options.bucket

    try:
        ext = os.path.splitext(options.overlay_options.video_file.key)[1][1:]
        key, destination_file = generate_random_filename(key_prefix, ext=ext)
        source_file = download_file(
            options.overlay_options.video_file.key,
            options.overlay_options.video_file.bucket,
        )
        destination_file = generate_text_overlay(
            source_file, destination_file, options
        )
        if destination_file:
            msg = "Generating a new Text Overlay Clip is succesfull."
            msg = f"{msg} File: {destination_file}"
            log.info(msg)
            meta_dict = get_clip_meta(local_file_path=destination_file)
            meta_reponse = get_meta_response_scheme(meta_dict)
            file_operations = upload_and_remove(destination_file, key, bucket)
            remove_tmp_file(source_file)

            if not file_operations:
                raise Exception("File Operation Error")

            response_scheme = get_response_scheme(meta_reponse, key)
            return response_scheme

    except Exception as exc:
        msg = f"Error while generating Color Clip Reason: {exc}"
        log.error(msg)

    return None


def color_clip(options):
    """Short summary.

    Parameters
    ----------
    options : ColorClipParams
        Description of parameter `options`.

    Returns
    -------
    ResponseSchema
        Description of returned object.

    """
    # ReturnFileOptions

    key_prefix = "color_clips"
    bucket = DEFAULT_BUCKET

    if options.output_options:
        key_prefix = options.output_options.key_prefix
        if options.output_options.bucket:
            bucket = options.output_options.bucket

    try:
        ext = "mp4"
        key, destination_file = generate_random_filename(key_prefix, ext=ext)
        destination_file = generate_color_clip(destination_file, options)
        if destination_file:
            msg = "Generating a new Color Clip is succesfull."
            msg = f"{msg} File: {destination_file}"
            log.info(msg)
            meta_dict = get_clip_meta(local_file_path=destination_file)
            meta_reponse = get_meta_response_scheme(meta_dict)
            file_operations = upload_and_remove(destination_file, key, bucket)

            if not file_operations:
                raise Exception("File Operation Error")

            response_scheme = get_response_scheme(meta_reponse, key)
            return response_scheme

    except Exception as exc:
        msg = f"Error while generating Color Clip Reason: {exc}"
        log.error(msg)

    return None


def encode_clip(options):
    """Short summary.

    Parameters
    ----------
    options : EncodeParams
        Description of parameter `options`.

    Returns
    -------
    ResponseSchema
        Description of returned object.

    """
    # ReturnFileOptions

    key_prefix = "encoded_clips"
    bucket = DEFAULT_BUCKET

    if options.output_options:
        key_prefix = options.output_options.key_prefix
        if options.output_options.bucket:
            bucket = options.output_options.bucket

    try:
        ext = os.path.splitext(options.video_file.key)[1][1:]
        key, destination_file = generate_random_filename(key_prefix, ext=ext)
        source_file = download_file(
            options.video_file.key, options.video_file.bucket
        )
        destination_file = encode(source_file, destination_file, options)
        if destination_file:
            msg = "Encoding process is succesfull."
            msg = f"{msg} File: {destination_file}"
            log.info(msg)
            meta_dict = get_clip_meta(local_file_path=destination_file)
            remove_tmp_file(source_file)
            meta_reponse = get_meta_response_scheme(meta_dict)
            file_operations = upload_and_remove(destination_file, key, bucket)

            if not file_operations:
                raise Exception("Encoding Operation Error")

            response_scheme = get_response_scheme(meta_reponse, key)
            return response_scheme

    except Exception as exc:
        msg = f"Error while encoding the clip Reason: {exc}"
        log.error(msg)

    return None


def concat_clips(options):
    """Short summary.

    Parameters
    ----------
    options : EncodeParams
        Description of parameter `options`.

    Returns
    -------
    ResponseSchema
        Description of returned object.

    """
    # ReturnFileOptions

    key_prefix = "concat_clips"
    bucket = DEFAULT_BUCKET

    if options.output_options:
        key_prefix = options.output_options.key_prefix
        if options.output_options.bucket:
            bucket = options.output_options.bucket

    try:
        ext = os.path.splitext(options.video_files[0].key)[1][1:]
        key, destination_file = generate_random_filename(key_prefix, ext=ext)
        infile_list = list()
        for vid_file in options.video_files:
            infile_list.append(download_file(vid_file.key, vid_file.bucket))

        destination_file = concat_videos(
            infile_list, destination_file, options
        )
        if destination_file:
            msg = "Concat process is succesfull."
            msg = f"{msg} File: {destination_file}"
            log.info(msg)
            meta_dict = get_clip_meta(local_file_path=destination_file)
            [remove_tmp_file(i) for i in infile_list]
            meta_reponse = get_meta_response_scheme(meta_dict)
            file_operations = upload_and_remove(destination_file, key, bucket)

            if not file_operations:
                raise Exception("Concat Operation Error")

            response_scheme = get_response_scheme(meta_reponse, key)
            return response_scheme

    except Exception as exc:
        msg = f"Error while concatting clips. Reason: {exc}"
        log.error(msg)

    return None


def subclip(options):
    """Short summary.

    Parameters
    ----------
    options : EncodeParams
        Description of parameter `options`.

    Returns
    -------
    ResponseSchema
        Description of returned object.

    """
    # ReturnFileOptions

    key_prefix = "subclips"
    bucket = DEFAULT_BUCKET

    if options.output_options:
        key_prefix = options.output_options.key_prefix
        if options.output_options.bucket:
            bucket = options.output_options.bucket

    try:
        ext = os.path.splitext(options.video_file.key)[1][1:]
        key, destination_file = generate_random_filename(key_prefix, ext=ext)
        source_file = download_file(
            options.video_file.key, options.video_file.bucket
        )
        destination_file = create_subclip(
            source_file, destination_file, options
        )
        if destination_file:
            msg = "Subclip process is succesfull."
            msg = f"{msg} File: {destination_file}"
            log.info(msg)
            meta_dict = get_clip_meta(local_file_path=destination_file)
            remove_tmp_file(source_file)
            meta_reponse = get_meta_response_scheme(meta_dict)
            file_operations = upload_and_remove(destination_file, key, bucket)

            if not file_operations:
                raise Exception("Subclip Operation Error")

            response_scheme = get_response_scheme(meta_reponse, key)
            return response_scheme

    except Exception as exc:
        msg = f"Error while creating subclip. Reason: {exc}"
        log.error(msg)

    return None


def create_hls(options):
    """Short summary.

    Parameters
    ----------
    options : EncodeParams
        Description of parameter `options`.

    Returns
    -------
    ResponseSchema
        Description of returned object.

    """
    # ReturnFileOptions

    key_prefix = "hls_clips"
    bucket = DEFAULT_BUCKET

    if options.output_options:
        key_prefix = options.output_options.key_prefix
        if options.output_options.bucket:
            bucket = options.output_options.bucket

    try:
        key, destination_file = generate_random_filename(
            key_prefix, ext="m3u8"
        )
        source_file = download_file(
            options.video_file.key, options.video_file.bucket
        )
        destination_file, ts_files = create_hls_from_vid(
            source_file, destination_file, options
        )
        if destination_file:
            msg = "HLS process is succesful."
            msg = f"{msg} File: {destination_file}"
            log.info(msg)
            # Can't get correct meta from m3u8, so use the first ts instead
            meta_dict = get_clip_meta(local_file_path=ts_files[0][0])
            remove_tmp_file(source_file)
            meta_reponse = get_meta_response_scheme(meta_dict)
            file_operations = upload_and_remove(destination_file, key, bucket)
            ts_file_ops = [
                upload_and_remove(i[0], f"{key_prefix}/{i[1]}", bucket)
                for i in ts_files
            ]

            if not file_operations:
                raise Exception("HLS Operation Error")
            if not all(ts_file_ops):
                raise Exception("Not all TS files are uploaded.")

            response_scheme = get_response_scheme(meta_reponse, key)
            return response_scheme

    except Exception as exc:
        msg = f"Error while creating HLS Stream. Reason: {exc}"
        log.error(msg)

    return None


def sound_mix(options):
    # ReturnFileOptions

    key_prefix = "sound_mix_clips"
    bucket = DEFAULT_BUCKET

    if options.output_options:
        key_prefix = options.output_options.key_prefix
        if options.output_options.bucket:
            bucket = options.output_options.bucket

    try:
        ext = os.path.splitext(options.overlay_options.video_file.key)[1][1:]
        key, destination_file = generate_random_filename(key_prefix, ext=ext)
        source_file = download_file(
            options.overlay_options.video_file.key,
            options.overlay_options.video_file.bucket,
        )
        sound_file = download_file(
            options.sound_file.key, options.sound_file.bucket
        )
        destination_file = generate_sound_mix_clip(
            source_file, sound_file, destination_file, options
        )
        if destination_file:
            msg = "Generating a new Sound Mixed clip is succesfull."
            msg = f"{msg} File: {destination_file}"
            log.info(msg)
            meta_dict = get_clip_meta(local_file_path=destination_file)
            meta_reponse = get_meta_response_scheme(meta_dict)
            file_operations = upload_and_remove(destination_file, key, bucket)
            remove_tmp_file(source_file)
            remove_tmp_file(sound_file)

            if not file_operations:
                raise Exception("File Operation Error")

            response_scheme = get_response_scheme(meta_reponse, key)
            return response_scheme

    except Exception as exc:
        msg = f"Error while generating Sound Mixed clip. Reason: {exc}"
        log.error(msg)

    return None


def create_frame(options):
    # ReturnFileOptions

    key_prefix = "frames"
    bucket = DEFAULT_BUCKET

    if options.output_options:
        key_prefix = options.output_options.key_prefix
        if options.output_options.bucket:
            bucket = options.output_options.bucket

    try:
        key, destination_file = generate_random_filename(key_prefix, ext="png")
        source_file = download_file(
            options.video_file.key,
            options.video_file.bucket,
        )
        destination_file = generate_frame_from_video(
            source_file, destination_file, options
        )
        if destination_file:
            msg = "Generating Frame from video is succesfull."
            msg = f"{msg} File: {destination_file}"
            log.info(msg)
            meta_dict = get_clip_meta(local_file_path=destination_file)
            meta_reponse = get_meta_response_scheme(meta_dict)
            file_operations = upload_and_remove(destination_file, key, bucket)
            remove_tmp_file(source_file)

            if not file_operations:
                raise Exception("File Operation Error")

            response_scheme = get_response_scheme(meta_reponse, key)
            return response_scheme

    except Exception as exc:
        msg = f"Error while generating Sound Mixed clip. Reason: {exc}"
        log.error(msg)

    return None


def create_tts(options):
    # ReturnFileOptions

    key_prefix = "text-to-speech"
    bucket = DEFAULT_BUCKET

    if options.output_options:
        key_prefix = options.output_options.key_prefix
        if options.output_options.bucket:
            bucket = options.output_options.bucket

    try:
        # Wav is better?
        key, destination_file = generate_random_filename(key_prefix, ext="mp3")
        destination_file = create_tts_file(destination_file, options)
        if destination_file:
            msg = "Generating Text to Speech sound file is successful"
            msg = f"{msg} File: {destination_file}"
            log.info(msg)
            file_operations = upload_and_remove(destination_file, key, bucket)

            if not file_operations:
                raise Exception("File Operation Error")

            response_scheme = get_response_scheme(None, key)
            return response_scheme

    except Exception as exc:
        msg = f"Error while generating text to speech file. Reason: {exc}"
        log.error(msg)

    return None


def create_pvi(options):
    # ReturnFileOptions

    key_prefix = "pvi_clips"
    bucket = "mustapv"

    if options.output_options:
        key_prefix = options.output_options.key_prefix
        if options.output_options.bucket:
            bucket = options.output_options.bucket

    try:
        key, destination_file = generate_random_filename(
            key_prefix, ext="m3u8"
        )
        if options.custom_link is not None:
            destination_file = TEMP_DIR + "/" + options.custom_link
            key = key_prefix + "/" + options.custom_link
        source_file = download_file(
            options.m3u8_file.key,
            options.m3u8_file.bucket,
        )
        destination_file = create_pvi_clips(
            source_file, destination_file, options
        )
        if destination_file:
            msg = "Generating PVI is Successful"
            msg = f"{msg} File: {destination_file}"
            log.info(msg)
            file_operations = [
                upload_and_remove(destination_file[key], key, bucket)
                for key in destination_file.keys()
            ]
            # file_operations = upload_and_remove(destination_file, key, bucket)

            if not file_operations:
                raise Exception("File Operation Error")

            response_scheme = get_response_scheme(None, key)
            remove_tmp_file(source_file)
            return response_scheme

    except Exception as exc:
        msg = f"Error while generating PVI clips. Reason: {exc}"
        log.error(msg)

    return None
