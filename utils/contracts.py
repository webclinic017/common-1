from datetime import date, datetime, timedelta
import pandas as pd
import ring
import time
from tqdm import tqdm

from ..data.eikon import get_timeseries
from ..constants import LETTERS
from ..constants import FUTURES
from ..data.database import get_chain, ric_exists


@ring.lru()
def get_contract(stem, day, contract_rank=0):
    chain = get_chain(stem, day)
    contract = chain.iloc[contract_rank, :]
    ltd = datetime.strptime(
        contract.LTD, '%Y-%m-%d').date()
    ric = contract.RIC if ric_exists(contract.RIC) \
        else contract.RIC.split('^')[0]
    return ltd, ric


def get_front_contract(day, stem):
    future = FUTURES.get(stem, {})
    roll_offset_from_reference = \
        timedelta(days=future.get('RollOffsetFromReference', -31))
    reference_day = day - roll_offset_from_reference
    return get_contract(
        stem=stem,
        day=reference_day,
        contract_rank=0)


def get_next_contract(day, stem):
    future = FUTURES.get(stem, {})
    roll_offset_from_reference = \
        timedelta(days=future.get('RollOffsetFromReference', -31))
    reference_day = day - roll_offset_from_reference
    return get_contract(
        stem=stem,
        day=reference_day,
        contract_rank=1)


def list_existing_maturities(stem, start_date, end_date):
    data_dict = {}
    err = {}
    delta = end_date - start_date

    datas = {}
    for i in tqdm(range(delta.days + 1)):
        day = start_date + timedelta(days=i)
        if LETTERS[day.month-1] not in FUTURES[stem].get('NormalMonths', []):
            continue
        ric = to_outright(stem, day.year, day.month, is_active=False)
        if ric not in datas:
            r = get_timeseries(rics=ric,
                               fields=['OPEN', 'HIGH',
                                       'LOW', 'CLOSE', 'VOLUME'],
                               start_date=start_date.isoformat(),
                               end_date=end_date.isoformat(),
                               interval='daily')
            datas[ric] = r['data']
            time.sleep(1)
        data = datas[ric]
        if data is None or len(data) == 0:
            if ric not in err:
                err[ric] = day.isoformat()
            continue
        df = pd.DataFrame(data)
        df['Date'] = df['Date'].apply(lambda x: pd.to_datetime(x, unit='ms'))
        df.set_index('Date', inplace=True)
        if ric not in data_dict:
            data_dict[ric] = {
                'YearMonth': df.index[-1].date().strftime('%Y-%m'),
                'FND': None,
                'LTD': df.index[-1].date().isoformat(),
                'RIC': ric,
                'WeTrd': 1,
            }
    data = list(data_dict.values())
    return data, err


def to_short_maturity(maturity):
    """Convert a long maturity (example M24) into a short maturity (M4).

    :param maturity: Maturity to be converted
    :return: Returns a short maturity
    """
    return '{}{}'.format(maturity[0], maturity[2])


def stem_to_ric_from_year(stem, year, month, is_active):
    letter = LETTERS[month-1]
    maturity = f'{letter}{(year % 100):02d}'
    formatted_maturity = to_short_maturity(maturity)
    reuters_stem = FUTURES[stem]['Stem']['Reuters']
    ric = f'{reuters_stem}{formatted_maturity}'
    if not is_active:
        return '{}^{}'.format(ric, maturity[1])
    return ric


@ring.lru()
def to_outright(stem, year, month, day=None, is_active=False):
    if day is None:
        return stem_to_ric_from_year(stem, year, month, is_active)
    if date.today() > date(year, month, day) + timedelta(days=10):
        return stem_to_ric_from_year(stem, year, month, is_active=False)
    ric = stem_to_ric_from_year(stem, year, month, is_active=False)
    if ric_exists(ric):
        return ric
    return stem_to_ric_from_year(stem, year, month, is_active=True)
