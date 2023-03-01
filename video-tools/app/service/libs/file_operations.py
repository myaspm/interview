from os import remove
from os.path import exists

import shutil

from conf.config import TEMP_DIR, DEFAULT_BUCKET
from conf.log import log

from libs.s3_utils import write_to_s3fs, get_from_s3fs


def download_file(key, bucket=DEFAULT_BUCKET):
    """Short summary.

    Parameters
    ----------
    key : str
        Description of parameter `key`.
    bucket : str
        Description of parameter `bucket`.

    Returns
    -------
    str
        Description of returned object.

    """

    file_path = get_from_s3fs(bucket=bucket, key=key, local_path=TEMP_DIR)
    if not file_path:
        msg = f"Could not create local file at {TEMP_DIR}/{key}"
        raise Exception(msg)

    msg = f"Download completed. Object written at {file_path}"
    log.info(msg)
    return file_path


def upload_file(local_file_path, key, bucket=DEFAULT_BUCKET):
    """Short summary.

    Parameters
    ----------
    local_file : str
        Description of parameter `key`.
    key : str
        Description of parameter `key`.
    bucket : str
        Description of parameter `bucket`.

    Returns
    -------
    bool
        Description of returned object.

    """
    try:
        with open(local_file_path, "rb") as local_file:
            write_to_s3fs(local_file.read(), bucket, key)

        msg = f"Upload completed. Bucket: {bucket} Key: {key}"
        log.info(msg)

        return True

    except Exception as exc:
        msg = f"Upload failed. Bucket: {bucket} Key: {key} Reason:{exc}"
        log.error(msg)

    return False


def remove_tmp_file(local_file_path, temp_dir=TEMP_DIR):
    """Short summary.

    Parameters
    ----------
    local_file_path : str
        Full file path
    temp_dir: str
        Temp files folder

    Returns
    -------
    bool
        Description of returned object.

    """
    try:
        if exists(local_file_path):
            remove(local_file_path)
            msg = f"Object deleted from {local_file_path}"
            log.debug(msg)
            local_key = local_file_path.replace(f"{temp_dir}/", "")
            folders = local_key.split("/")
            if len(folders) > 1:
                # the key has folders and remove the folders
                shutil.rmtree(f"{temp_dir}/{folders[0]}")

            return True

    except Exception as exc:
        msg = (
            f"Error while deleting object from {local_file_path} Reason: {exc}"
        )
        log.error(msg)

    return False
