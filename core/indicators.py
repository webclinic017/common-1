import numpy as np
import pandas as pd

from ..core.forex import Forex

REFERENCE_TWELVE_MONTHS_VOLATILITY = 0.2
SIX_BUSINESS_MONTHS = 125
SIX_MONTHS = 183
TWELVE_MONTHS = 250


class Indicators():

    def __init__(self):
        self.bars = {}
        self.forex = Forex()
        self.trading_days = {}

    def get_momentum(self, stem, window=SIX_BUSINESS_MONTHS):
        if stem not in self.bars:
            return np.NaN
        bars = self.bars[stem]
        if bars.shape[0] < window:
            return np.NaN
        _return = bars.CLOSE[-1] / bars.CLOSE[-window] - 1
        return _return * self.get_volatility_weight(stem, window)

    def get_volatility_weight(self, stem, window=SIX_BUSINESS_MONTHS):
        if stem not in self.bars:
            return np.NaN
        bars = self.bars[stem]
        if bars.shape[0] < window:
            return np.NaN
        std = np.std(np.diff(np.log(bars.CLOSE[-window:-1])))
        weight = REFERENCE_TWELVE_MONTHS_VOLATILITY / \
            (std * np.sqrt(TWELVE_MONTHS))
        return weight

    def set_bar(self, stem, bar):
        bar = self.forex.bar_to_usd(bar, stem)
        if stem not in self.bars:
            self.bars[stem] = bar
        else:
            self.bars[stem] = pd.concat([self.bars[stem], bar])

    def should_trade_today(self, day, stem, frequency='monthly'):
        if frequency == 'monthly':
            key = day.strftime('%Y-%m')
        elif frequency == 'weekly':
            key = day.strftime('%Y-%U')
        trading_days = self.trading_days.get(stem, [])
        if key not in trading_days:
            self.trading_days[stem] = trading_days + [key]
            return True
        return False
