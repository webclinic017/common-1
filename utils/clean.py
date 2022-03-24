import json
import tempfile

import click
from common.data.minio_client import exists_object, fget_object, minio_client, remove_object


@click.command()
@click.option('--bucket-name', required=True, default='')
def main(bucket_name):
    for object in minio_client.list_objects(bucket_name, recursive=True):
        object_name = object.object_name
        if not exists_object(bucket_name, object_name):
            continue
        with tempfile.NamedTemporaryFile(suffix='.json') as temp_file:
            fget_object(bucket_name, object_name, temp_file.name)
            with open(temp_file.name, 'r') as f:
                r = json.load(f)
        if r['data'] is None and r['error'] is None:
            remove_object(bucket_name, object_name)
            print('remove_object', bucket_name, object_name)


if __name__ == '__main__':
     main()
