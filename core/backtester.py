from datetime import datetime, timedelta
import os
import uuid

import numpy as np
import pandas as pd
from pprint import pprint
from tqdm import tqdm

from ..constants import COIN_TYPE, FUTURE_TYPE, STOCK_TYPE
from ..data.gdrive import DATE, get_cash, get_executions, \
    get_positions, get_returns, save_table
from ..core.broker import Broker
from ..utils.contracts import get_chain
from ..utils.gmail import send_email
from ..utils.dates import is_weekend
from ..utils.sms import send_sms
from ..core.indicators import TWELVE_MONTHS
from ..core.market_data import MarketDataFuture


class Backtester():

    def __init__(self, stems, start_date, end_date, cash, leverage,
                 live, instrument_type=FUTURE_TYPE, no_check=False, plot=True, suffix=''):
        self.broker = Broker(cash, live, no_check)
        self.market_data = MarketDataFuture()
        self.cash = cash
        self.data = []
        self.dates = []
        self.day = None
        self.end_date = end_date
        self.instrument_type = instrument_type
        self.leverage = leverage
        self.live = live
        self.nav = cash
        self.plot = plot
        self.start_date = start_date
        self.stems = stems
        self.suffix = suffix

    def adjust_positions(self, spread):
        day, cash = get_cash(spread=spread)
        nav = pd.DataFrame(data=self.data, index=self.dates)
        index = pd.to_datetime(nav.index, format='%Y-%m-%d').get_loc(
            datetime.combine(day, datetime.min.time()), method='nearest')
        nav_of_day = nav.at[nav.index[index], 'Nav']
        adjustment_ratio = cash / nav_of_day
        self.broker.apply_adjustment(adjustment_ratio)
        self.nav = self.broker.nav

    def compute_kelly(self):
        dfm = pd.DataFrame(data=self.data, index=self.dates)
        returns = np.diff(np.log(dfm.Nav))
        kelly = np.nanmean(returns) / np.power(np.nanstd(returns), 2)
        return kelly

    def compute_mean(self):
        dfm = pd.DataFrame(data=self.data, index=self.dates)
        returns = np.diff(np.log(dfm.Nav))
        mean = np.nanmean(returns) * TWELVE_MONTHS
        return mean

    def plot_nav(self):
        dfm = pd.DataFrame(data=self.data, index=self.dates)
        fname = ','.join(self.stems)
        if len(fname) > 30:
            fname = str(uuid.uuid3(uuid.NAMESPACE_URL, fname))
        filename = fname + \
            f'.{self.leverage}.{self.end_date.isoformat()}.{self.suffix}.png'
        path = os.path.join(os.getenv('HOME'), 'Downloads', filename)
        dfm[['Nav']].plot(logy=True).get_figure().savefig(path)

    def compute_sharpe_ratio(self):
        dfm = pd.DataFrame(data=self.data, index=self.dates)
        returns = np.diff(np.log(dfm.Nav))
        sharpe_ratio = np.nanmean(returns) / np.nanstd(returns) * np.sqrt(250)
        return sharpe_ratio

    def compute_std(self):
        dfm = pd.DataFrame(data=self.data, index=self.dates)
        returns = np.diff(np.log(dfm.Nav))
        std = np.nanstd(returns) * np.sqrt(TWELVE_MONTHS)
        return std

    def has_not_enough_active_contracts(self):
        for stem in self.stems:
            active_contracts = get_chain(stem, self.day)
            if active_contracts.shape[0] < 2:
                return stem
        return None

    def run(self):
        delta = self.end_date - self.start_date
        for i in tqdm(range(delta.days + 1)):
            self.day = self.start_date + timedelta(days=i)
            if self.instrument_type == FUTURE_TYPE:
                not_enough_active_contracts = self.has_not_enough_active_contracts()
                if not_enough_active_contracts is not None:
                    raise Exception(
                        f'Update future-expiry/{not_enough_active_contracts}.csv in Minio')
            if is_weekend(self.day):
                continue
            self.broker.next(self.day)
            self.next()
            self.next_indicators()
            self.dates.append(self.day)
            nav = self.broker.nav
            if not np.isnan(nav):
                self.nav = nav
            data = {}
            data['Nav'] = nav
            self.data.append(data)
        if not self.live and self.plot:
            self.plot_nav()
        kelly = self.compute_kelly()
        mean = self.compute_mean()
        sharpe = self.compute_sharpe_ratio()
        std = self.compute_std()
        pprint({
            'mean': mean,
            'std': std,
            'kelly': kelly,
            'sharpe': sharpe,
        })

    def next(self):
        pass

    def next_indicators(self):
        pass

    def get_current_positions(self, keep_zero):
        data = {}
        for asset_type in [COIN_TYPE, FUTURE_TYPE, STOCK_TYPE]:
            for key, value in self.broker.positions.get(asset_type, {}).items():
                if value != 0 or keep_zero:
                    data[key] = value
        current_positions = pd.DataFrame(data=[data])
        current_positions.insert(0, DATE, self.day.isoformat())
        return current_positions

    def save(self, spread, should_send_sms=True):
        self.save_positions_and_send_sms(spread, should_send_sms)
        self.save_returns(spread)
        self.save_executions(spread)

    def save_executions(self, spread):
        executions = pd.DataFrame(data=self.broker.executions)
        if len(executions) == 0:
            return
        previous_executions = get_executions(spread=spread)
        index = executions.Date > previous_executions.Date.tolist()[-1]
        executions = executions.loc[index, :]
        all_executions = pd.concat(
            [previous_executions, executions], sort=True)
        save_table(all_executions, spread=spread,
                   sheet='Executions')

    def save_returns(self, spread):
        previous_returns = get_returns(spread=spread)
        returns = pd.DataFrame({
            'Date': [d.isoformat() for d in self.dates[:-1]],
            'Strategy': np.diff(np.log([d['Nav'] for d in self.data]))
        })
        index = returns.Date > previous_returns.Date.tolist()[-1]
        returns = returns.loc[index, :]
        all_returns = pd.concat([previous_returns, returns], sort=True)
        save_table(all_returns, spread=spread,
                   sheet='Returns')

    def save_positions_and_send_sms(self, spread, should_send_sms):
        previous_positions = get_positions(spread=spread)
        index = previous_positions.Date < self.day.isoformat()
        previous_positions = previous_positions.loc[index, :]
        all_positions = pd.concat(
            [previous_positions, self.get_current_positions(keep_zero=True)], sort=True)
        all_positions = all_positions[[DATE]
                                      + [col for col in all_positions.columns if col != DATE]]
        all_positions = all_positions.replace(np.nan, 0)
        columns = ['Date'] + \
            sorted([c for c in all_positions.columns if c != 'Date'])
        all_positions = all_positions.reindex(columns, axis=1)
        save_table(all_positions, spread=spread, sheet='All Positions')
        current_positions = all_positions.iloc[-2:, ]
        non_zero_columns = (current_positions != 0).any(axis=0)
        current_positions = current_positions.loc[:, non_zero_columns]
        save_table(current_positions, spread=spread,
                   sheet='Current Positions')
        if len(all_positions) < 2:
            return
        if should_send_sms and self.broker.has_execution:
            current_positions = self.get_current_positions(keep_zero=False)
            send_sms(f'[INFO] New trade for {spread}')
            text = '\n'.join([col + ': ' + str(current_positions.loc[:, col].iloc[0])
                              for col in current_positions.columns])
            send_email(
                f'[INFO] New trade for {spread}', text)
