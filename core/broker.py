import numpy as np

from ..core.commissions import COMMISSION_INTERACTIVE_BROKERS_USD
from ..constants import FUTURES
from ..data.database import get_future_ohlcv_for_day, ric_to_stem
from ..core.backtester import FUTURE_TYPE
from ..core.forex import Forex
from ..core.margin import Margin
from ..core.market_data import MarketData
from ..core.market_impact import MarketImpact
from ..utils.contracts import get_front_contract, get_next_contract


class Broker():

    def __init__(self, cash, live, no_check=False,):
        self.positions = {
            'Cash': {
                'AUD': 0,
                'CAD': 0,
                'CHF': 0,
                'EUR': 0,
                'GBP': 0,
                'HKD': 0,
                'JPY': 0,
                'SGD': 0,
                'USD': cash,
            },
            FUTURE_TYPE: {},
        }
        self.previous_close = {}
        self.day = None
        self.executions = []
        self.forex = Forex()
        self.has_execution = False
        self.live = live
        self.margin = Margin(instrument_type=FUTURE_TYPE)
        self.market_data = MarketData()
        self.market_impact = MarketImpact()
        self.no_check = no_check

    def apply_adjustment(self, adjustment_ratio):
        for key in self.positions.keys():
            self.positions[key] = {k: v * adjustment_ratio
                                   for k, v in self.positions[key].items()}

    def apply_commission(self, contract_number):
        commission = -np.abs(contract_number) * \
            COMMISSION_INTERACTIVE_BROKERS_USD
        self.positions['Cash']['USD'] += commission
        return commission

    def apply_market_impact(self, ric, contract_number, execution_price):
        relative_market_impact = self.market_impact.get(ric=ric)
        stem = ric_to_stem(ric)
        full_point_value = FUTURES[stem]['FullPointValue']
        currency = FUTURES[stem]['Currency']
        full_point_value_usd = full_point_value * \
            self.forex.to_usd(currency, self.day)
        market_impact = -np.abs(contract_number) * relative_market_impact * \
            execution_price * full_point_value_usd
        self.positions['Cash'][currency] += market_impact
        return market_impact

    def buy_future(self, ric, contract_number):
        if not self.live:
            contract_number = np.round(contract_number)
        if contract_number == 0:
            return
        stem = ric_to_stem(ric)
        currency = FUTURES[stem]['Currency']
        if np.isnan(self.positions['Cash'][currency]):
            raise ValueError('Cash is nan.', ric, self.day)
        row = self.market_data.bardata(ric=ric, day=self.day)
        if np.isnan(row.CLOSE[0]):
            raise ValueError('Close is nan.', ric, self.day)
        execution_price = row.CLOSE[0]
        self.positions[FUTURE_TYPE][ric] = self.positions[FUTURE_TYPE].get(
            ric, 0) + contract_number
        self.positions['Cash'][currency] -= contract_number * \
            execution_price * FUTURES[stem]['FullPointValue']
        commission = self.apply_commission(contract_number)
        market_impact = self.apply_market_impact(
            ric, contract_number, execution_price)
        self.check_initial_margin(ric, contract_number)
        self.executions.append({
            **{
                'Date': self.day.isoformat(),
                'Ric': ric,
                'Stem': stem,
                'Type': 'Buy' if contract_number > 0 else 'Sell',
                'ContractNumber': contract_number,
                'Currency': currency,
                'ExecutionPrice': execution_price,
                'FullPointValue': FUTURES[stem]['FullPointValue'],
                'Commission': commission,
                'MarketImpact': market_impact,
            }, **{
                f'CashAfter{k}': v for k, v in self.positions['Cash'].items()
            }
        })
        self.has_execution = True

    def check_initial_margin(self, ric, contract_number):
        if self.no_check:
            return
        total_required_margin = 0
        for _ric, _contract_number in self.positions[FUTURE_TYPE].items():
            if _ric == ric:
                _contract_number += contract_number
            stem = ric_to_stem(_ric)
            margin = self.margin.overnight_maintenance_future(stem, self.day) \
                if _ric != ric else self.margin.overnight_initial_future(stem, self.day)
            total_required_margin += np.abs(_contract_number) * margin
        if total_required_margin > self.nav:
            raise Exception(
                f'Initial margin exceeded {self.day.isoformat()} {ric} {contract_number} {total_required_margin} {self.nav}')

    def check_maintenance_margin(self):
        if self.no_check:
            return
        total_required_margin = 0
        for _ric, _contract_number in self.positions[FUTURE_TYPE].items():
            stem = ric_to_stem(_ric)
            margin = self.margin.overnight_maintenance_future(stem, self.day)
            if np.isnan(margin):
                continue
            total_required_margin += np.abs(_contract_number) * margin
        if total_required_margin > self.nav:
            raise Exception(
                f'Maintenance margin exceeded {self.day.isoformat()}')

    def expire_future(self, ric):
        df, _ = get_future_ohlcv_for_day(day=self.day, ric=ric)
        execution_price = df.CLOSE[0] if not np.isnan(df.CLOSE[0]) \
            else np.nanmedian(df[['OPEN', 'HIGH', 'LOW']])
        return self.close_future(ric, execution_price)

    def close_future(self, ric, execution_price=None):
        stem = ric_to_stem(ric)
        currency = FUTURES[stem]['Currency']
        if np.isnan(self.positions['Cash'][currency]):
            raise ValueError('Cash is nan.', ric, self.day)
        if execution_price is None:
            row = self.market_data.bardata(ric=ric, day=self.day)
            if np.isnan(row.CLOSE[0]):
                raise ValueError('Close is nan.', ric, self.day)
            execution_price = row.CLOSE[0]
        contract_number = self.positions[FUTURE_TYPE].get(ric, 0)
        self.positions[FUTURE_TYPE][ric] = self.positions[FUTURE_TYPE].get(
            ric, 0) - contract_number
        stem = ric_to_stem(ric)
        self.positions['Cash'][currency] += contract_number * \
            execution_price * FUTURES[stem]['FullPointValue']
        commission = self.apply_commission(contract_number)
        market_impact = self.apply_market_impact(
            ric, contract_number, execution_price)
        self.executions.append({
            **{
                'Date': self.day.isoformat(),
                'Ric': ric,
                'Stem': stem,
                'Type': 'Close',
                'ContractNumber': contract_number,
                'Currency': currency,
                'ExecutionPrice': execution_price,
                'FullPointValue': FUTURES[stem]['FullPointValue'],
                'Commission': commission,
                'MarketImpact': market_impact,
            }, **{
                f'CashAfter{k}': v for k, v in self.positions['Cash'].items()
            }
        })
        self.has_execution = True
        return contract_number

    @property
    def nav(self):
        if np.any([np.isnan(cash) for cash in self.positions['Cash'].values()]):
            raise ValueError('Cash is nan.', self.day)
        cash_in_usd = np.sum([value * self.forex.to_usd(currency, self.day)
                              for currency, value in self.positions['Cash'].items()])
        nav = cash_in_usd
        for ric, contract_number in self.positions[FUTURE_TYPE].items():
            if contract_number == 0:
                continue
            if self.market_data.is_trading_day(ric=ric, day=self.day):
                row = self.market_data.bardata(ric=ric, day=self.day)
                close = row.CLOSE[0]
                self.previous_close[ric] = close
            else:
                close = self.previous_close.get(ric, np.NaN)
            stem = ric_to_stem(ric)
            full_point_value = FUTURES[stem]['FullPointValue']
            currency = FUTURES[stem]['Currency']
            full_point_value_usd = full_point_value * \
                self.forex.to_usd(currency, self.day)
            nav += contract_number * close * full_point_value_usd
        return nav

    def next(self, day):
        self.day = day
        self.check_maintenance_margin()
        self.has_execution = False

    def roll_front_contract(self, stem):
        _, front_ric = get_front_contract(day=self.day, stem=stem)
        _, next_ric = get_next_contract(day=self.day, stem=stem)
        if not self.market_data.is_trading_day(day=self.day, ric=front_ric) \
                or not self.market_data.is_trading_day(day=self.day, ric=next_ric):
            return
        closed_contract_number = 0
        if self.positions[FUTURE_TYPE].get(front_ric, 0) != 0:
            closed_contract_number += self.close_future(front_ric)

        if closed_contract_number != 0:
            self.buy_future(next_ric, closed_contract_number)
        return next_ric

    def sell_future(self, ric, contract_number):
        return self.buy_future(ric, -contract_number)

    def set_no_check(self, no_check):
        self.no_check = no_check
