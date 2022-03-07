import numpy as np

from ..constants import FUTURES
from ..utils.contracts import get_front_contract


class PositionSizingFuture():

    def __init__(self, broker, indicators, market_data):
        self.broker = broker
        self.indicators = indicators
        self.market_data = market_data
        self.nav = np.NaN

    def get_average_true_range_risk(self, stem, leverage):
        average_true_range = self.indicators.get_average_true_range(
            stem)
        if np.isnan(average_true_range):
            return 0
        _, front_ric = get_front_contract(
            day=self.broker.day,
            stem=stem)
        day = self.broker.day
        full_point_value = FUTURES[stem]['FullPointValue']
        currency = FUTURES[stem]['Currency']
        full_point_value_usd = full_point_value * \
            self.broker.forex.to_usd(currency, day)
        contract_number = self.nav * 0.002 / \
            (average_true_range * full_point_value_usd) * leverage
        return contract_number

    def get_volatility_risk(self, stem, leverage):
        volatility_weight = self.indicators.get_volatility_weight(stem)
        _, front_ric = get_front_contract(
            day=self.broker.day,
            stem=stem)
        day = self.broker.day
        row = self.market_data.bardata(ric=front_ric, day=day)
        close = row['CLOSE'][0]
        full_point_value = FUTURES[stem]['FullPointValue']
        currency = FUTURES[stem]['Currency']
        full_point_value_usd = full_point_value * \
            self.broker.forex.to_usd(currency, day)
        contract_number = self.nav * volatility_weight * \
            leverage / (close * full_point_value_usd)
        return contract_number

    def set_nav(self, nav):
        self.nav = nav
