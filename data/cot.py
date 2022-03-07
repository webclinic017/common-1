import json
import logging
import os
import tempfile
import ring

import pandas as pd
import quandl as qdl

from ..constants import FUTURES
from ..data.database import download_from_s3, json_data_to_df
from ..data.minio_client import exists_object, fget_object, put_object


# What are the different COT types
COT_TYPES = {
    'F': 'FuturesOnly',
    'FO': 'FuturesAndOptions',
    'F_L': 'FuturesOnlyLegacy',
    'FO_L': 'FuturesAndOptionsLegacy'
}

# Quandl API Key
qdl.ApiConfig.api_key = 'SkmBQRG9gxQK4HmeSoze'


@ring.lru()
def get_release_dates():
    """
    Get Report and Release Dates from the Release Dates file. 
    This is to be used as the COT data is usually prepared
    on Tuesday and released on Friday. The downloaded COT data 
    has the report date (Tuesday as a reference), where as we
    need the release date.

    :return: DataFrame - With ReportDate as index and ReleaseDate as column
    """
    bucket_name = 'daily-cot'
    object_name = f'ReleaseDates.txt'
    if not exists_object(bucket_name, object_name):
        raise Exception(f'No object {bucket_name}/{object_name} in S3')
    with tempfile.NamedTemporaryFile(suffix='.txt') as f:
        fget_object(bucket_name, object_name, f.name)
        dates = pd.read_csv(f.name, index_col=0, parse_dates=True)
    dates.index.names = [None]
    # Format Release Dates to Timestamp (as this is the original format and will be used later)!
    dates['ReleaseDate'] = pd.to_datetime(
        dates['ReleaseDate'], format='%Y-%m-%d')
    return dates


def next_release_date(date):
    """
    Get the next release date after the specified date.

    :param date: pandas.Timestamp - Last date
    :return: pandas.Timestamp - Next release date
    """
    df = get_release_dates()
    df = df[df['ReleaseDate'] > date]
    return df['ReleaseDate'].iloc[0]


#@ring.lru()
def download_commitment_of_traders(stem, cot_type='F'):
    if cot_type in COT_TYPES:
        qdl_code = FUTURES[stem]['COT']
        df = qdl.get('CFTC/{}_{}_ALL'.format(qdl_code, cot_type))
    else:
        raise Exception('COT Type {} not defined!'.format(cot_type))
    return df


def get_commitment_of_traders(stem, cot_type='F'):
    """
    Get the cot data and cache it (Refresh it if file is older than 7 days).
    COT Types can be:
        -- F: Futures Only
        -- FO: Futures And Options
        -- F_L: Futures Only Legacy
        -- FO_L Futures And Options Only

    :param stem: str -  Market stem (customized)
    :param cot_type: String COT Type
    :return: Dataframe with COT data
    """
    bucket_name = 'daily-cot'
    object_name = f'{stem}.json'
    df_quandl = download_commitment_of_traders(stem=stem, cot_type=cot_type)
    if exists_object(bucket_name, object_name):
        data, _ = download_from_s3(bucket_name, object_name)
        df_s3 = json_data_to_df(data, version='v1')
        df_quandl = df_quandl.loc[df_quandl.index > df_s3.index[-1], ]
        df_concat = pd.concat([df_s3, df_quandl], sort=True)
    else:
        df_concat = df_quandl
    data = json.loads(df_concat.reset_index(level=0).to_json(orient='records'))
    response = {
        'data': data,
        'error': {}
    }
    temp_dir = tempfile.TemporaryDirectory()
    path = os.path.join(temp_dir.name, object_name)
    with open(path, 'w') as f:
        json.dump(response, f)
    put_object(path, bucket_name)
    temp_dir.cleanup()
    return json_data_to_df(data, version='v1')


@ring.lru()
def cot_data(stem, cot_type='F'):
    """
    Get COT data and transform it!

    :param stem: String customized ticker
    :param cot_type: str - COT Type (see get_cot for descriptions)
    :param with_cit: Boolean Supplemental COT Data (Database not maintained)
    :return: DataFrame
    """
    log = logging.getLogger(__name__)

    # COT data
    cot = get_commitment_of_traders(stem, cot_type)
    if cot is not None:
        # Check if it is a commodity (commodities have only 16 columns)
        if len(cot.columns) == 16:
            # Calculations
            cot['ManagerNet'] = cot['Money Manager Longs'] - \
                cot['Money Manager Shorts']
            comms_long = cot['Producer/Merchant/Processor/User Longs'] + \
                cot['Swap Dealer Longs']
            comms_short = cot['Producer/Merchant/Processor/User Shorts'] + \
                cot['Swap Dealer Shorts']
            cot['HPH'] = (comms_long - comms_short) / \
                (comms_long + comms_short) * 100
            cot['HPS'] = cot['ManagerNet'] / \
                (cot['Money Manager Longs'] + cot['Money Manager Shorts']) * 100
            cot['RMN'] = cot['ManagerNet'] / cot['Open Interest'] * 100
            cot['RMS'] = cot['Money Manager Spreads'] / \
                cot['Open Interest'] * 100  # Is this useful?
            cot['HPSSlope'] = cot['HPS'].rolling(10).apply(
                lambda x: (x[-1] - x[0]) / len(x), raw=True)

        else:
            log.warning(
                'Implementation not finished for non-commodities futures COT! \
                 Only HPS Implemented!')
            specs_long = cot['Asset Manager Longs'] + \
                cot['Leveraged Funds Longs']
            specs_short = cot['Asset Manager Shorts'] + \
                cot['Leveraged Funds Shorts']
            cot['HPS'] = (specs_long - specs_short) / \
                (specs_long + specs_short) * 100
            cot['HPH'] = 0
            cot['RMN'] = 0
            cot['RMS'] = 0
            cot['HPSSlope'] = cot['HPS'].rolling(10).apply(
                lambda x: (x[-1] - x[0]) / len(x), raw=True)
        # Transform the date (from Report Date to Release Date)
        dates = get_release_dates()
        cot = cot.join(dates)
        cot.set_index('ReleaseDate', inplace=True)
        cot.index.names = [None]

    return cot
