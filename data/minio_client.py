
import os

from minio import Minio


minio_client = Minio(
    os.getenv('DATA_DOMAIN') + ':9000',
    access_key=os.getenv('MINIO_ROOT_USER'),
    secret_key=os.getenv('MINIO_ROOT_PASSWORD'),
    secure=True,
)


def copy_object(bucket_name, object_name, object_source,
                conditions=None, source_sse=None, sse=None, metadata=None):
    return minio_client.copy_object(bucket_name, object_name, object_source,
                                    conditions, source_sse, sse, metadata)


def exists_object(bucket_name, object_name):
    try:
        minio_client.stat_object(bucket_name, object_name)
        return True
    except:
        return False

def make_bucket_if_not_exists(bucket_name):
    buckets = minio_client.list_buckets()
    if bucket_name not in [b.name for b in buckets]:
        minio_client.make_bucket(bucket_name)


def put_object(path, bucket_name, object_name=None):
    file_stat = os.stat(path)
    object_name = os.path.basename(path) if not object_name else object_name
    with open(path, 'rb') as data:
        minio_client.put_object(
            bucket_name, object_name, data, file_stat.st_size, 'application/octet-stream')


def list_object_names(bucket_name):
    return [o.object_name for o in minio_client.list_objects(bucket_name)]


def fget_object(bucket_name, object_name, file_path):
    return minio_client.fget_object(bucket_name, object_name, file_path)


def remove_object(bucket_name, object_name):
    return minio_client.remove_object(bucket_name, object_name)

def stat_object(bucket_name, object_name):
    return minio_client.stat_object(bucket_name, object_name)

if __name__ == '__main__':
    from datetime import date
    BUCKET_NAME = 'daily-ohlcv'
    for obj in minio_client.list_objects(BUCKET_NAME):
        if obj.last_modified.date() == date.today():
            remove_object(BUCKET_NAME, object_name=obj.object_name)
