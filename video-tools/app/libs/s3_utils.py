"""Utility functions for S3 operations"""

from os import makedirs
from os.path import join, exists


from s3fs import S3FileSystem
from botocore.exceptions import EndpointConnectionError

from conf.log import log
from conf.config import AWS_ACCESS_SETTINGS, WORKER_ID, TEMP_DIR


def check_and_create_path(path):
    if not exists(path):
        makedirs(path)


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
            log.error("FS object is None")
    except ValueError as e:
        log.error(f"{str(e)}")
    except PermissionError:
        log.error("S3 Authentication Failed")
    except EndpointConnectionError:
        log.error("Can't Connect to S3 Endpoint")
    except Exception as e:
        log.error(f"Undefined Error: {str(e)}")


def get_s3fs_config():
    return get_s3fs(**AWS_ACCESS_SETTINGS)


def write_to_s3fs(data, bucket, key):

    fs = get_s3fs(**AWS_ACCESS_SETTINGS)

    # Check if bucket exists, create if not
    try:
        fs.ls(bucket)
    except OSError:
        fs.mkdir(bucket)
    except Exception:
        fs.mkdir(bucket)

    with fs.open(join(bucket, key), "wb") as out_file:
        out_file.write(data)


def get_from_s3fs(bucket, key, local_path=TEMP_DIR):
    try:
        fs = get_s3fs(**AWS_ACCESS_SETTINGS)
        key_path = ("/").join(key.split("/")[:-1])
        if key_path:
            # the key has folders and create the same folder structure
            check_and_create_path(f"{local_path}/{key_path}")

        with fs.open(join(bucket, key), "rb") as remote_file:
            local_key = f"{local_path}/{key}"
            with open(local_key, "wb") as in_file:
                in_file.write(remote_file.read())
        return local_key
    except Exception as e:
        log_error = f"Cannot retrieve object [{key}] in bucket [{bucket}]"
        log.error(f"[Worker: {WORKER_ID}] {log_error}: {str(e)}")
