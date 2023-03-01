from os.path import join

from s3fs import S3FileSystem
from botocore.exceptions import EndpointConnectionError

from log.log import log
from config.config import (
    AWS_ACCESS_SETTINGS,
    WORKER_ID,
)


def get_s3fs(**kwargs):
    try:
        fs = S3FileSystem(anon=False, client_kwargs=kwargs)
        if fs is not None:
            try:
                fs.ls("test/")
                return fs
            except FileNotFoundError:
                return fs
        else:
            log.exception(f"[Worker: {WORKER_ID}] FS object is None")
    except ValueError as e:
        log.exception(f"[Worker: {WORKER_ID}] {str(e)}")
    except PermissionError:
        log.exception(f"[Worker: {WORKER_ID}] S3 Authentication Failed")
    except EndpointConnectionError:
        log.exception(f"[Worker: {WORKER_ID}] Can't Connect to S3 Endpoint")
    except Exception as e:
        log.exception(f"[Worker: {WORKER_ID}] Undefined Error: {str(e)}")


def get_s3fs_config():
    return get_s3fs(**AWS_ACCESS_SETTINGS)


def write_audio_to_s3fs(audio_data, bucket, key):

    fs = get_s3fs(**AWS_ACCESS_SETTINGS)

    # Check if bucket exists, create if not
    try:
        fs.ls(bucket)
    except OSError:
        fs.mkdir(bucket)
    except Exception:
        fs.mkdir(bucket)

    with fs.open(join(bucket, key), "wb") as out_file:
        out_file.write(audio_data)
