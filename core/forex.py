from datetime import date
import numpy as np
import ring

from ..data.database import get_daily_forex_for_day
from ..constants import FUTURES


STOCK_CURRENCIES = [
    {
        'Currency': 'EUR',
        'RICSuffix': 'AS',
    },
    {
        'Currency': 'EUR',
        'RICSuffix': 'BR',
    },
    {
        'Currency': 'EUR',
        'RICSuffix': 'DE',
    },
    {
        'Currency': 'GBP',
        'RICSuffix': 'L',
    },
    {
        'Currency': 'EUR',
        'RICSuffix': 'MC',
    },
    {
        'Currency': 'EUR',
        'RICSuffix': 'MI',
    },
    {
        'Currency': 'EUR',
        'RICSuffix': 'PA',
    },
    {
        'Currency': 'CHF',
        'RICSuffix': 'S',
    },
]


class Forex:

    @staticmethod
    def bar_to_usd(bar, stem):
        currency = FUTURES[stem]['Currency']
        if currency != 'USD':
            day = bar.index[0]
            rate = Forex.to_usd(currency, day)
            columns = ['OPEN', 'HIGH', 'LOW', 'CLOSE']
            bar.loc[:, columns] = bar.loc[:, columns] * rate
            bar.loc[:, 'VOLUME'] = bar.loc[:, 'VOLUME'] / rate
        return bar

    @ring.lru()
    @staticmethod
    def to_usd(currency, day):
        if currency == 'AUD':
            return Forex.get_pair(day, 'audusd', 'USDAUD=R', invert=True)
        elif currency == 'CAD':
            return Forex.get_pair(day, 'cadusd', 'CADUSD=R')
        elif currency == 'CHF':
            return Forex.get_pair(day, 'chfusd', 'CHFUSD=R')
        elif currency == 'EUR':
            return Forex.get_pair(day, 'eurusd', 'USDEUR=R', invert=True)
        elif currency == 'GBP':
            return Forex.get_pair(day, 'gbpusd', 'USDGBP=R', invert=True)
        elif currency == 'HKD':
            return Forex.get_pair(day, 'hkdusd', 'HKDUSD=R')
        elif currency == 'JPY':
            return Forex.get_pair(day, 'jpyusd', 'JPYUSD=R')
        elif currency == 'USD':
            return 1
        elif currency == 'SGD':
            return Forex.get_pair(day, 'sgdusd', 'SGDUSD=R')
        return np.NaN

    @ring.lru()
    @staticmethod
    def get_pair(day, pair, ric, invert=False):
        df = get_daily_forex_for_day(day, ric)
        return 1 / df.CLOSE if invert else df.CLOSE

    @staticmethod
    def get_stock_currency(ric):
        splitted_ric = ric.split('.')
        if len(splitted_ric) == 1:
            return 'USD'
        else:
            ric_suffix = splitted_ric[-1]
            currencies = [sc['Currency']
                          for sc in STOCK_CURRENCIES if sc['RICSuffix'] == ric_suffix]
            assert len(currencies) == 1
            return currencies[0]


if __name__ == '__main__':
    day = date(2001, 1, 4)
    forex = Forex()
    print(forex.to_usd('AUD', day))
