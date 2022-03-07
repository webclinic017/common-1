import click
import json
import multiprocessing
import os
import pickle
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from tqdm import tqdm
import urllib.parse


EIKON_BASE_URL = 'https://' + os.getenv('EIKON_DOMAIN') + ':8000'
EIKON_SECRET_KEY = os.getenv('EIKON_SECRET_KEY')

retry_strategy = Retry(
    total=3,
    status_forcelist=[429, 500, 502, 503, 504],
    method_whitelist=["HEAD", "GET", "OPTIONS"]
)
adapter = HTTPAdapter(max_retries=retry_strategy)
http = requests.Session()
http.mount("https://", adapter)
http.mount("http://", adapter)


def get_data(instruments, fields, parameters=None, field_name: bool = False, raw_output: bool = False, debug: bool = False):
    instruments = urllib.parse.quote_plus(','.join(instruments) if isinstance(
        instruments, list) else instruments)
    fields = urllib.parse.quote_plus(
        ','.join(fields) if isinstance(fields, list) else fields)
    payload = {
        'parameters': urllib.parse.quote_plus(json.dumps(parameters)),
        'field_name': field_name,
        'raw_output': raw_output,
        'debug': debug
    }
    headers = {'Authorization': EIKON_SECRET_KEY}
    r = http.get(f'{EIKON_BASE_URL}/data/{instruments}/{fields}/',
                 headers=headers, params=payload)
    return r.json()


def get_news_headlines(query='Topic:TOPALL and Language:LEN', count=10, date_from=None,
                       date_to=None, raw_output: bool = False, debug: bool = False):
    payload = {
        'query': query,
        'count': count,
        'date_from': date_from,
        'date_to': date_to,
        'raw_output': raw_output,
        'debug': debug
    }
    headers = {'Authorization': EIKON_SECRET_KEY}
    r = http.get(f'{EIKON_BASE_URL}/news_headlines/',
                 headers=headers, params=payload)
    return r.json()


def get_news_story(story_id, raw_output: bool = False, debug: bool = False):
    payload = {
        'raw_output': raw_output,
        'debug': debug
    }
    headers = {'Authorization': EIKON_SECRET_KEY}
    r = http.get(f'{EIKON_BASE_URL}/news_story/{story_id}/',
                 headers=headers, params=payload)
    return r.json()


def get_symbology(symbol, from_symbol_type='RIC', to_symbol_type=None, raw_output: bool = False, debug: bool = False, bestMatch: bool = True):
    payload = {
        'from_symbol_type': from_symbol_type,
        'to_symbol_type': to_symbol_type,
        'raw_output': raw_output,
        'debug': debug,
        'bestMatch': bestMatch
    }
    headers = {'Authorization': EIKON_SECRET_KEY}
    r = http.get(f'{EIKON_BASE_URL}/symbology/{symbol}/',
                 headers=headers, params=payload)
    return r.json()


def get_timeseries(rics, fields='*', start_date=None, end_date=None,
                   interval='daily', count=None,
                   calendar=None, corax=None, normalize: bool = False, raw_output: bool = False, debug: bool = False):
    rics = urllib.parse.quote_plus(
        ','.join(rics) if isinstance(rics, list) else rics)
    fields = urllib.parse.quote_plus(
        ','.join(fields) if isinstance(fields, list) else fields)
    payload = {
        'fields': fields,
        'start_date': start_date,
        'end_date': end_date,
        'interval': interval,
        'count': count,
        'calendar': calendar,
        'corax': corax,
        'normalize': normalize,
        'raw_output': raw_output,
        'debug': debug
    }
    headers = {'Authorization': EIKON_SECRET_KEY}
    r = http.get(f'{EIKON_BASE_URL}/timeseries/{rics}/',
                 headers=headers, params=payload)
    return r.json()


def clean(fp):
    with open(fp, 'rb') as f:
        data = pickle.load(f)
        if data is None:
            os.unlink(fp)


def clean_cache(folders):
    folders = folders.split(',')
    pickle_files = []
    for folder in folders:
        path = os.path.join(os.getenv('OPENCTA_DATA_PATH'),
                            'lab', 'raw', 'eikon', folder)
        pickle_files += [os.path.join(dp, f)
                         for dp, dn, filenames in os.walk(path)
                         for f in filenames if os.path.splitext(f)[1] == '.pickle']
    with multiprocessing.Pool(multiprocessing.cpu_count()) as p:
        list(tqdm(p.imap(clean, pickle_files), total=len(pickle_files)))
    print()


@click.command()
@click.option('--mode', default='clean_cache')
@click.option('--folders', default='get_data,get_timeseries')
def main(mode, folders):
    print(mode)
    if mode == 'clean_cache':
        clean_cache(folders)


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
