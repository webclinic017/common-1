import click
from datetime import date, datetime
import os
import pandas as pd
from pprint import pprint

from ..constants import FUTURES
from ..utils.contracts import list_existing_maturities
from ..data.database import download_expiry_from_s3

EIKON = 'eikon'
S3 = 's3'


def download_expiry(stem, source):
    if source == EIKON:
        return download_expiry_from_eikon(stem, end_date=date.today())
    elif source == S3:
        return download_expiry_from_s3(stem)


def download_expiry_from_eikon(stem, end_date=date.today()):
    start_date_str = FUTURES.get(stem, {}).get('StartDate', '1980-01-01')
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
    data, err = list_existing_maturities(stem, start_date, end_date)
    pprint(err)
    df = pd.DataFrame(data=data)
    return df


@click.command()
@click.option('--stems', default='MGC', required=True)
def list_and_save_existing_maturities(stems):
    stems = stems.split(',') if stems else list(FUTURES.keys())
    end_date = date.today()
    for stem in stems:
        print(stem)
        start_date_str = FUTURES.get(
            stem, {}).get('StartDate', '1980-01-01')
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
        data, _ = list_existing_maturities(stem, start_date, end_date)
        df = pd.DataFrame(data=data)
        path = os.path.join(os.getenv('HOME'), 'Downloads', f'{stem}.csv')
        df.to_csv(path, index=False)


if __name__ == '__main__':
    list_and_save_existing_maturities()  # pylint: disable=no-value-for-parameter
