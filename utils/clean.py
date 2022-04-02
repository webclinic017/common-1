import json
import tempfile

import click
from common.data.minio_client import exists_object, fget_object, minio_client, remove_object
from tqdm import tqdm


@click.command()
@click.option('--bucket-name', required=True, default='')
def main(bucket_name):
    for object in tqdm(list(minio_client.list_objects(bucket_name, recursive=True))):
        object_name = object.object_name
        if not exists_object(bucket_name, object_name):
            continue
        if not object_name.endswith('.json'):
            continue
        with tempfile.NamedTemporaryFile(suffix='.json') as temp_file:
            fget_object(bucket_name, object_name, temp_file.name)
            with open(temp_file.name, 'r') as f:
                r = json.load(f)
        too_many_requests = isinstance(r, dict) \
            and 'data' in r and r['data'] is None \
            and 'error' in r and r['error'] is None
        eikon_not_running = isinstance(r, dict) \
            and 'data' in r and r['data'] is None \
            and 'error' in r and r['error'] is not None \
            and 'Eikon Proxy not running or cannot be reached.' in r['error']['message']
        if too_many_requests or eikon_not_running:
            remove_object(bucket_name, object_name)
            print('remove_object', bucket_name, object_name)


if __name__ == '__main__':
    '''
    for data in coinapi cot dividend forex libor news-headlines news-story ohlcv taq; do
      echo "daily-${data}"
      python clean.py --bucket-name daily-${data}
    done
    '''
    main()
