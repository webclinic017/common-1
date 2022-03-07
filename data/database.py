from datetime import date, datetime, timedelta
import json
import os
import tempfile

import numpy as np
import pandas as pd
import ring

from ..constants import FUTURES, LIBOR_BEFORE_2001, MAXIMUM_REQUEST_SIZE, START_DATE
from ..data.eikon import get_data, get_news_headlines, get_news_story, get_timeseries
from ..data.minio_client import exists_object, fget_object, put_object
from ..utils.files import ensure_dir


@ring.lru()
def download_expiry_from_s3(stem):
    bucket_name = 'future-expiry'
    object_name = f'{stem}.csv'
    if not exists_object(bucket_name, object_name):
        raise Exception(f'No object {bucket_name}/{object_name} in S3')
    with tempfile.NamedTemporaryFile(suffix='.csv') as f_in:
        fget_object(bucket_name, object_name, f_in.name)
        return pd.read_csv(f_in.name)


@ring.lru()
def get_chain(stem, day=START_DATE, minimum_time_to_expiry=0):
    df = download_expiry_from_s3(stem)
    if datetime.strptime(df.LTD.iloc[-1], '%Y-%m-%d').date() - day < timedelta(days=minimum_time_to_expiry):
        source = get_expiry_calendar(stem)
        raise Exception(
            f'Not enough data for {stem}. Download expiry data from {source}')
    index = (df.LTD >= day.isoformat()) & (df.WeTrd == 1)
    return df.loc[index, :].reset_index(drop=True)


def get_expiry_calendar(stem):
    return FUTURES.get(stem, {}).get('ExpiryCalendar', '')


def get_instrument_parameters(stem):
    return FUTURES.get(stem, {})


def get_start_date(stem):
    start_date = FUTURES.get(stem, {}).get('StartDate', '1980-01-01')
    return datetime.strptime(start_date, '%Y-%m-%d').date()


def json_data_to_df(data, version='v1'):
    df = pd.DataFrame(data)
    if version == 'v1':
        if 'Date' in list(df.columns):
            df['Date'] = pd.to_datetime(df['Date'], unit='ms')
            df.set_index('Date', inplace=True)
    elif version == 'v2':
        if 'Date' in list(df.columns):
            df['Date'] = df['Date'].str[:10]
            df.set_index('Date', inplace=True)
            del df['index']
    elif version == 'v3':
        if 'versionCreated' in list(df.columns):
            df['versionCreated'] = pd.to_datetime(
                df['versionCreated'], unit='ms')
            df.set_index('versionCreated', inplace=True)
    elif version == 'v4':
        df = df.rename(columns={
            'price_open': 'OPEN',
            'price_high': 'HIGH',
            'price_low': 'LOW',
            'price_close': 'CLOSE',
            'volume_traded': 'VOLUME',
            'time_period_start': 'Date'
        })
        df.loc[:, 'Date'] = df.Date.apply(
            lambda x: datetime.strptime(x[:10], '%Y-%m-%d'))
        df = df[['Date', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME']]
        df.set_index('Date', inplace=True)
    return df


@ring.lru()
def ric_exists(ric):
    r = get_data(instruments=[ric], fields=['TR.RIC', 'CF_NAME'])
    try:
        return r['data'][0]['RIC'] is not None
    except:
        return False


@ring.lru()
def get_last_trade_date(ric):
    stem = ric_to_stem(ric)
    chain = get_chain(stem)
    outright_ric = ric if is_outright(ric) else get_outright_ric(ric)
    if '^' not in outright_ric:
        suffix = str(date.today().year)[2]
        outright_ric += f'^{suffix}'
    contract = chain.loc[chain.RIC == outright_ric, :]
    if contract.shape[0] == 1:
        return datetime.strptime(contract.LTD.values[0], '%Y-%m-%d').date()
    return None


@ring.lru()
def get_is_active(ric, last_trade_date=None, day=date.today()):
    if last_trade_date is None:
        last_trade_date = get_last_trade_date(ric)
    if last_trade_date >= day:
        return True
    if last_trade_date < day - timedelta(days=30):
        return False
    return not ric_exists(ric)


@ring.lru()
def will_expire_soon(ric, day=date.today()):
    last_trade_date = get_last_trade_date(ric)
    return day > last_trade_date - timedelta(days=10)


def download_all_daily_ohlcv_from_eikon(ric, start_date, end_date):
    delta = end_date - start_date
    data = []
    for i in range(0, delta.days + 1, MAXIMUM_REQUEST_SIZE):
        start_date_of_slice = start_date + timedelta(days=i)
        end_date_of_slice = start_date + \
            timedelta(days=min(i + MAXIMUM_REQUEST_SIZE -
                               1, delta.days))
        r = get_timeseries(rics=ric,
                           fields=['OPEN', 'HIGH',
                                   'LOW', 'CLOSE', 'VOLUME'],
                           start_date=start_date_of_slice.isoformat(),
                           end_date=end_date_of_slice.isoformat(),
                           interval='daily')
        if r['data'] is not None:
            data += r['data']
    r = {'data': data, 'error': r['error']}
    bucket_name = 'daily-ohlcv'
    object_name = f'{ric}.json'
    temp_dir = tempfile.TemporaryDirectory()
    path = os.path.join(temp_dir.name, object_name)
    with open(path, 'w') as f:
        json.dump(r, f)
    put_object(path, bucket_name)
    temp_dir.cleanup()


@ring.lru()
def download_daily_ohlcv_from_eikon(ric, start_date, last_trade_date):
    r = get_timeseries(rics=ric,
                       fields=['OPEN', 'HIGH',
                               'LOW', 'CLOSE', 'VOLUME'],
                       start_date=start_date.isoformat(),
                       end_date=last_trade_date.isoformat(),
                       interval='daily')
    bucket_name = 'daily-ohlcv'
    object_name = f'{ric}.json'
    temp_dir = tempfile.TemporaryDirectory()
    path = os.path.join(temp_dir.name, object_name)
    with open(path, 'w') as f:
        json.dump(r, f)
    put_object(path, bucket_name)
    temp_dir.cleanup()
    return r['data'], r['error']


@ring.lru()
def download_daily_news_headlines_from_eikon(ric, day):
    r = get_news_headlines(
        query=ric,
        date_from=day.isoformat(),
        date_to=(day + timedelta(days=1)).isoformat(),
        count=50)
    bucket_name = 'daily-news-headlines'
    object_name = f'{day.isoformat()}/{ric}.json'
    temp_dir = tempfile.TemporaryDirectory()
    path = os.path.join(temp_dir.name, object_name)
    with open(ensure_dir(path), 'w') as f:
        json.dump(r, f)
    put_object(path, bucket_name, object_name)
    temp_dir.cleanup()
    return r['data'], r['error']


@ring.lru()
def download_daily_news_story_from_eikon(story_id):
    r = get_news_story(story_id=story_id)
    bucket_name = 'daily-news-story'
    object_name = f'{story_id}.json'
    temp_dir = tempfile.TemporaryDirectory()
    path = os.path.join(temp_dir.name, object_name)
    with open(ensure_dir(path), 'w') as f:
        json.dump(r, f)
    put_object(path, bucket_name, object_name)
    temp_dir.cleanup()
    return r['data'], r['error']


@ring.lru()
def download_from_s3(bucket_name, object_name, version='v1'):
    if not exists_object(bucket_name, object_name):
        return None, None
    with tempfile.NamedTemporaryFile(suffix='.json') as temp_file:
        fget_object(bucket_name, object_name, temp_file.name)
        with open(temp_file.name, 'r') as f:
            r = json.load(f)
    if version == 'v1':
        return r['data'], r['error']
    elif version == 'v2':
        return r, {'message': f'No OHLCV for {object_name}'} if len(r) == 0 else None


def get_stock_news_headlines_for_day(day, ric=None):
    bucket_name = 'daily-news-headlines'
    object_name = f'{day.isoformat()}/{ric}.json'
    data, err = download_from_s3(bucket_name, object_name)
    if data is None:
        data, err = download_daily_news_headlines_from_eikon(ric, day)
    return json_data_to_df(data, version='v3'), None


def get_stock_news_story_for_day(story_id):
    bucket_name = 'daily-news-story'
    object_name = f'{story_id}.json'
    data, err = download_from_s3(bucket_name, object_name)
    if data is None:
        data, err = download_daily_news_story_from_eikon(story_id)
    return data, err


def get_coin_ohlcv_for_day(day, ric=None):
    bucket_name = 'daily-coinapi'
    object_name = f'{day.isoformat()}/{ric}-USD.json'
    data, err = download_from_s3(bucket_name, object_name, version='v2')
    if err:
        if 'No OHLCV for' in err['message']:
            return None, err
    if data is None:
        return data, {'message': f'No OHLCV for {object_name}'}
    return json_data_to_df(data, version='v4'), None


def get_future_ohlcv(ric):
    bucket_name = 'daily-ohlcv'
    object_name = f'{ric}.json'
    data, err = download_from_s3(bucket_name, object_name)
    if err:
        if 'Invalid RIC' in err['message']:
            return None, err
    df = json_data_to_df(data)
    return df, None


def get_future_ohlcv_for_day(day, contract_rank=0, ric=None, stem=None):
    ric, stem = get_ric_and_stem(
        day=day, contract_rank=contract_rank, ric=ric, stem=stem)
    start_date = get_start_date(stem)
    if day < start_date:
        message = f'[not-started] Future {stem} starts on {start_date.isoformat()}'
        return None, {'message': message}
    last_trade_date = get_last_trade_date(ric)
    if last_trade_date is None or day > last_trade_date:
        message = f'No OHLCV for {ric} on {day.isoformat()}'
        return None, {'message': message}
    bucket_name = 'daily-ohlcv'
    object_name = f'{ric}.json'
    data, err = download_from_s3(bucket_name, object_name)
    if err:
        if 'Invalid RIC' in err['message']:
            return None, err
    df = json_data_to_df(data)
    if df is not None:
        index = df.index == day.isoformat()
        current_day_exists = np.any(index)
        days_after_exist = np.any(df.index > day.isoformat())
        if current_day_exists:
            return df.loc[index, :], None
        elif days_after_exist:
            message = f'No OHLCV for {ric} on {day.isoformat()}'
            return None, {'message': message}
    data, _ = download_daily_ohlcv_from_eikon(ric, start_date, last_trade_date)
    df = json_data_to_df(data)
    index = df.index == day.isoformat()
    if not np.any(index):
        message = f'No OHLCV for {ric} on {day.isoformat()}'
        return None, {'message': message}
    return df.loc[index, :], None


def get_stock_ohlcv_for_day(day, ric=None):
    bucket_name = 'daily-ohlcv'
    object_name = f'{ric}.json'
    data, err = download_from_s3(bucket_name, object_name)
    if err:
        if 'Invalid RIC' in err['message']:
            return None, err
    df = json_data_to_df(data)
    if df is not None:
        index = df.index == day.isoformat()
        current_day_exists = np.any(index)
        days_after_exist = np.any(df.index > day.isoformat())
        if current_day_exists:
            return df.loc[index, :], None
        elif days_after_exist:
            message = f'No OHLCV for {ric} on {day.isoformat()}'
            return None, {'message': message}
    data, _ = download_daily_ohlcv_from_eikon(ric, START_DATE, date.today())
    df = json_data_to_df(data)
    index = df.index == day.isoformat()
    if not np.any(index):
        message = f'No OHLCV for {ric} on {day.isoformat()}'
        return None, {'message': message}
    return df.loc[index, :], None


@ring.lru()
def download_daily_libor_from_eikon(ric):
    start_date = START_DATE
    end_date = date.today()
    r = get_data(instruments=ric,
                 fields=[
                     'TR.FIXINGVALUE.Date', 'TR.FIXINGVALUE'],
                 parameters={'SDate': start_date.isoformat(), 'EDate': end_date.isoformat()})
    bucket_name = 'daily-libor'
    object_name = f'{ric}.json'
    temp_dir = tempfile.TemporaryDirectory()
    path = os.path.join(temp_dir.name, object_name)
    with open(path, 'w') as f:
        json.dump(r, f)
    put_object(path, bucket_name)
    temp_dir.cleanup()
    return r['data'], r['error']


@ring.lru()
def get_daily_libor_for_day(day):
    if day < date(2001, 1, 2):
        return LIBOR_BEFORE_2001
    ric = 'USDONFSR=X'
    bucket_name = 'daily-libor'
    object_name = f'{ric}.json'
    data, _ = download_from_s3(bucket_name, object_name)
    df = json_data_to_df(data, version='v2')
    if df is None or np.all(df.index < day.isoformat()):
        df, _ = download_daily_libor_from_eikon(ric)
    index = pd.to_datetime(df.index, format='%Y-%m-%d').get_loc(
        datetime.combine(day, datetime.min.time()), method='nearest')
    fixing_value = df.at[df.index[index], 'Fixing Value']
    return fixing_value


@ring.lru()
def get_ric_and_stem(day, contract_rank=0, ric=None, stem=None):
    ric = ric if ric else stem_to_ric(contract_rank, day, stem)
    stem = stem if stem else ric_to_stem(ric)
    chain = get_chain(stem)
    contract = chain.loc[chain.RIC == ric, :]
    if contract.shape[0] == 1:
        last_trade_date = datetime.strptime(
            contract.LTD.values[0], '%Y-%m-%d').date()
        is_active = get_is_active(ric, last_trade_date, date.today())
        ric = ric.split('^')[0] if is_active else ric
    return ric, stem


@ring.lru()
def download_daily_forex_from_eikon(ric, start_date, end_date, object_name):
    r = get_timeseries(rics=ric,
                       fields=['OPEN', 'HIGH',
                               'LOW', 'CLOSE'],
                       start_date=start_date.isoformat(),
                       end_date=end_date.isoformat(),
                       interval='daily')
    bucket_name = 'daily-forex'
    temp_dir = tempfile.TemporaryDirectory()
    path = os.path.join(temp_dir.name, object_name)
    with open(path, 'w') as f:
        json.dump(r, f)
    put_object(path, bucket_name)
    temp_dir.cleanup()
    return r['data'], r['error']


@ring.lru()
def get_daily_forex_for_day(day, ric):
    start_date = date(day.year, 1, 1)
    end_date = date(day.year, 12, 31)
    bucket_name = 'daily-forex'
    object_name = f'{ric}.{day.year}.json'
    data, _ = download_from_s3(bucket_name, object_name)
    df = json_data_to_df(data)
    if df is None or np.all(df.index < day.isoformat()):
        data, _ = download_daily_forex_from_eikon(
            ric, start_date, end_date, object_name)
        df = json_data_to_df(data)
    index = pd.to_datetime(df.index, format='%Y-%m-%d').get_loc(
        datetime.combine(day, datetime.min.time()), method='nearest')
    return df.iloc[index, :]


@ring.lru()
def ric_to_stem(ric):
    if not is_outright(ric):
        ric = get_outright_ric(ric)
    suffix = '^'
    stem_wo_suffix = ric.split(suffix)[0] if suffix in ric else ric
    delayed_data_prefix = '/'
    stem_wo_prefix = stem_wo_suffix.split(delayed_data_prefix)[-1] \
        if delayed_data_prefix in stem_wo_suffix else stem_wo_suffix
    stem_wo_year = ''.join([c for c in stem_wo_prefix if not c.isdigit()])
    stem_wo_month = stem_wo_year[:-1]
    if stem_wo_month in ['SIRT']:
        return 'SI'
    for stem in FUTURES.keys():
        if stem_wo_month == FUTURES[stem].get('Stem', {}).get('Reuters'):
            return stem


@ring.lru()
def stem_to_ric(contract_rank, day, stem):
    chain = get_chain(stem, day)
    contract = chain.iloc[contract_rank, :]
    return contract.RIC


def get_outright_ric(ric):
    if '-' not in ric:
        return ric
    start_index = 1 if ric[0].isdigit() else 0
    outright_ric = ric.split('-')[0][start_index:]
    if '^' in ric:
        outright_ric += '^' + ric.split('^')[1]
    if outright_ric.startswith('SIRT'):
        outright_ric = outright_ric[:2] + outright_ric[4:]
    return outright_ric


def is_outright(ric):
    return '-' not in ric


@ring.lru()
def get_last_trade_date(ric):
    ric_outright = get_outright_ric(ric)
    stem = ric_to_stem(ric_outright)
    chain = get_chain(stem)
    if '^' in ric_outright:
        contracts = chain.loc[chain.RIC == ric_outright, 'LTD']
        if contracts.shape[0] == 0:
            return None
        last_trade_date = datetime.strptime(
            contracts.iloc[0], '%Y-%m-%d').date()
    else:
        index = chain.RIC.apply(lambda x: x.split('^')[0]) == ric_outright
        contracts = chain.loc[index, :]
        if contracts.shape[0] == 0:
            return None
        ltd = min(contracts.LTD, key=lambda x:
                  abs(datetime.strptime(x, '%Y-%m-%d').date() - date.today()))
        last_trade_date = datetime.strptime(ltd, '%Y-%m-%d').date()
    return last_trade_date


if __name__ == '__main__':
    # print(get_daily_libor_for_day(date(2020, 12, 18)))
    # print(get_daily_forex_for_day(day=date(2020, 12, 18), ric='USDEUR=R'))
    print(get_stock_news_headlines_for_day(
        day=date(2021, 5, 10), ric='BNPP.PA'))
