from datetime import date, datetime, timedelta
import logging
import os
from pprint import pprint

import click
import pandas as pd

from ...constants import FUTURE_TYPE
from ...data.gdrive import get_cash
from ...core.indicators import TWELVE_MONTHS
from ...core.market_data import MarketData
from ...core.market_impact import MarketImpact
from ...constants import FUTURES
from ...strategies.sell_and_hold.constants import MINIMUM_START_DATE, BUY_AND_HOLD_STEMS
from ...strategies.sell_and_hold.models.backtester import SellAndHoldBacktester
from ...utils.contracts import list_existing_maturities

"""
KO: JRB,JRU,NG,SB
"""


def run(mode, stems, leverage):
    stems = stems.split(',')
    parameters = {
        'number_of_positions': len(stems)
    }
    if mode == 'backtest':
        start_date = MINIMUM_START_DATE
        end_date = date.today() - timedelta(days=1)
        cash = 1000000
        backtester = SellAndHoldBacktester(
            stems, start_date, end_date, cash, leverage, parameters)
        backtester.run()
        print(f'SR: {backtester.compute_sharpe_ratio()}')
        print(f'Kelly: {backtester.compute_kelly()}')
        print(f'NAV: {backtester.nav}')
        print(f'Positions {backtester.day}')
        positions = backtester.broker.positions
        positions[FUTURE_TYPE] = {k: v for k,
                                  v in positions[FUTURE_TYPE].items() if v != 0}
        pprint(positions)
    elif mode == 'live':
        end_date = date.today() - timedelta(days=1)
        start_date = max(
            MINIMUM_START_DATE,
            MarketData.get_start_day(first_trading_day=end_date, window=TWELVE_MONTHS))
        _, cash = get_cash(spread='Buy And Hold')
        backtester = SellAndHoldBacktester(
            stems, start_date, end_date, cash, leverage, parameters, live=True)
        backtester.run()
        backtester.adjust_positions(spread='Buy And Hold')
        backtester.save(spread='Buy And Hold', should_send_sms=True)
    elif mode == 'list_existing_maturities':
        stems = stems.split(',') if stems else list(FUTURES.keys())
        end_date = date.today()
        for stem in stems:
            start_date_str = FUTURES.get(
                stem, {}).get('StartDate', '1980-01-01')
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
            data, err = list_existing_maturities(stem, start_date, end_date)
            logging.info(stem)
            pprint(err)
            df = pd.DataFrame(data=data)
            path = os.path.join(os.getenv('HOME'), 'Downloads', f'{stem}.csv')
            df.to_csv(path, index=False)
    elif mode == 'market_impact':
        stems = stems.split(',') if stems else list(FUTURES.keys())
        start_date = date(2020, 8, 15)
        end_date = date(2020, 9, 15)
        market_impact = MarketImpact(start_date, end_date)
        cache = {}
        for stem in stems:
            cache[stem] = market_impact.get(stem)
        pprint(cache)


@ click.command()
@ click.option('--mode', required=True)
@ click.option('--stems', default=','.join(BUY_AND_HOLD_STEMS), required=True)
@ click.option('--leverage', default=4.0, required=True)
def main(mode, stems, leverage):
    run(mode, stems, leverage)


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
