"""Short summary.

Notes
-----
    Short summary.

"""


from uuid import uuid4
from conf.config import WORKER_ID, DEFAULT_BUCKET, TEMP_DIR
from service.libs.file_operations import (
    upload_file,
    remove_tmp_file,
)
from service.libs.models import MetaResponseSchema, S3File, ResponseSchema


def generate_random_filename(prefix, ext="mp4", folder=TEMP_DIR):
    """Short summary.

    Parameters
    ----------
    ext : str
        Description of parameter `ext`.
    folder : str
        Description of parameter `folder`.

    Returns
    -------
    str
        Description of returned object.

    """
    key = f"{str(uuid4())}.{ext}"
    local_key = key
    if folder:
        local_key = f"{folder}/{key}"

    key = f"{prefix}/{key}"

    return key, local_key


def get_meta_response_scheme(meta_dict):
    """Short summary.

    Parameters
    ----------
    meta_dict : Dict
        Description of parameter `meta_dict`.

    Returns
    -------
    MetaResponseSchema
        Description of returned object.

    """
    meta_response = MetaResponseSchema(**meta_dict)
    return meta_response


def get_response_scheme(
    meta_response, key, bucket=DEFAULT_BUCKET, worker=WORKER_ID
):
    """Short summary.

    Parameters
    ----------
    meta_response : MetaResponseSchema
        Description of parameter `meta_response`.
    key : str
        Description of parameter `key`.
    bucket : str
        Description of parameter `bucket`.
    worker : str
        Description of parameter `worker`.

    Returns
    -------
    ResponseSchema
        Description of returned object.

    """
    s3_file = S3File(key=key, bucket=bucket)
    response = {
        "video_file": s3_file,
        "created_by": worker,
        "metadata": meta_response,
    }
    response_scheme = ResponseSchema(**response)

    return response_scheme


def upload_and_remove(local_file, key, bucket=DEFAULT_BUCKET):
    """Short summary.

    Parameters
    ----------
    local_file : str
        Description of parameter `local_file`.
    key : str
        Description of parameter `key`.

    Returns
    -------
    bool
        Description of returned object.

    """
    upload_res = upload_file(local_file, key, bucket)
    remove_file_res = remove_tmp_file(local_file)

    return all([upload_res, remove_file_res])
