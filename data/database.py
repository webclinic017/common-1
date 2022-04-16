from datetime import date, datetime, timedelta
import json
import os
import tempfile

import numpy as np
import pandas as pd
import requests
import ring

from ..constants import FUTURES, LIBOR_BEFORE_2001, START_DATE
from ..data.minio_client import exists_object, fget_object, put_object
from ..utils.files import ensure_dir


class Client():
    def __init__(self):
        self.headers={
            'Authorization': os.getenv('DATA_SECRET_KEY')
        }
    
    def get_daily_factor(self, path, ticker, start_date, end_date):
        response = requests.get(
            f'http://localhost:8000/daily/factor/{path}',
            headers=self.headers,
            params={
                'ticker': ticker,
                'start_date': start_date,
                'end_date': end_date,
            })
        response_json = response.json()
        error = response_json['error']
        if error is not None:
            print(error)
        data = response_json['data']
        if data is None:
            return
        dfm = pd.DataFrame.from_dict(data)
        dfm = dfm.set_index(['Date', 'Stem'])
        return dfm
    
    def get_daily_ohlcv(self, ric, start_date, end_date):
        response = requests.get(
            f'http://localhost:8000/daily/ohlcv',
            headers=self.headers,
            params={
                'ric': ric,
                'start_date': start_date,
                'end_date': end_date,
            })
        response_json = response.json()
        print(response_json)
        error = response_json['error']
        if error is not None:
            print(error)
        data = response_json['data']
        if data is None:
            return
        dfm = pd.DataFrame.from_dict(data)
        dfm = dfm.set_index(['Date', 'RIC'])
        return dfm
    
    def get_daily_risk_free_rate(self, ric, start_date, end_date):
        response = requests.get(
            f'http://localhost:8000/daily/risk-free-rate',
            headers=self.headers,
            params={
                'ric': ric,
                'start_date': start_date,
                'end_date': end_date,
            })
        response_json = response.json()
        error = response_json['error']
        if error is not None:
            print(error)
        data = response_json['data']
        if data is None:
            return
        dfm = pd.DataFrame.from_dict(data)
        dfm = dfm.set_index(['Date', 'RIC'])
        return dfm
    
    def get_health_ric(self, ric):
        response = requests.get(
            f'http://localhost:8000/health/ric',
            headers=self.headers,
            params={
                'ric': ric,
            })
        data = response.json().get('data', False)
        return data
    
    def get_tickers(self):
        response = requests.get(
            f'http://localhost:8000/tickers',
            headers=self.headers)
        data = response.json().get('data', [])
        return data


client = Client()


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
    return client.get_health_ric(ric)


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
    dfm = client.get_daily_ohlcv(ric, start_date, last_trade_date)
    if dfm is not None:
        index = dfm.index == day.isoformat()
        current_day_exists = np.any(index)
        days_after_exist = np.any(dfm.index > day.isoformat())
        if current_day_exists:
            return dfm.loc[index, :], None
    message = f'No OHLCV for {ric} on {day.isoformat()}'
    return None, {'message': message}
    

@ring.lru()
def download_daily_libor_from_eikon(ric):
    start_date = START_DATE
    end_date = date.today()
    dfm = client.get_daily_risk_free_rate(ric, start_date, end_date)
    return dfm, None


@ring.lru()
def get_daily_libor_for_day(day):
    if day < date(2001, 1, 2):
        return LIBOR_BEFORE_2001
    ric = 'USDONFSR=X'
    df, _ = download_daily_libor_from_eikon(ric)
    index_date = df.index.map(lambda x: x[0])
    index = pd.to_datetime(index_date, format='%Y-%m-%d').get_loc(
        datetime.combine(day, datetime.min.time()), method='nearest')
    fixing_value = df.at[df.index[index], 'FixingValue']
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
def download_daily_forex_from_eikon(ric, start_date, end_date):
    dfm = client.get_daily_ohlcv(ric, start_date, end_date)
    return dfm, None


@ring.lru()
def get_daily_forex_for_day(day, ric):
    start_date = date(day.year, 1, 1)
    end_date = date(day.year, 12, 31)
    dfm = client.get_daily_ohlcv(ric, start_date, end_date)
    index = pd.to_datetime(dfm.index, format='%Y-%m-%d').get_loc(
        datetime.combine(day, datetime.min.time()), method='nearest')
    return dfm.iloc[index, :]


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
    print(get_daily_libor_for_day(date(2020, 12, 18)))
    #print(get_daily_forex_for_day(day=date(2020, 12, 18), ric='USDEUR=R'))
