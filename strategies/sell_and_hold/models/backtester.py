from ..constants import FUTURE_TYPE
from ....core.backtester import Backtester
from ....constants import FUTURES
from ....data.database import will_expire_soon
from ....utils.contracts import get_front_contract


class SellAndHoldBacktester(Backtester):

    def __init__(self, stems, start_date, end_date, cash, leverage, parameters, live=False, no_check=False, plot=True, suffix=''):
        super(SellAndHoldBacktester, self).__init__(
            stems, start_date, end_date, cash, leverage, live,
            instrument_type=FUTURE_TYPE, no_check=no_check, plot=plot, suffix=suffix)
        self.parameters = parameters

    def next(self):
        for stem in self.stems:
            _, front_ric = get_front_contract(
                stem=stem,
                day=self.broker.day)
            if not self.market_data.is_trading_day(day=self.day, ric=front_ric):
                continue
            if self.market_data.should_roll_today(self.day, stem):
                self.broker.roll_front_contract(stem)
            else:
                position = self.broker.positions[FUTURE_TYPE].get(
                    front_ric, 0)
                row = self.market_data.bardata(ric=front_ric, day=self.day)
                close = row['CLOSE'][0]
                if position == 0:
                    full_point_value = FUTURES[stem]['FullPointValue']
                    currency = FUTURES[stem]['Currency']
                    full_point_value_usd = full_point_value * \
                        self.broker.forex.to_usd(currency, self.day)
                    number_of_positions = self.parameters['number_of_positions']
                    contract_number = int(self.nav * self.leverage /
                                          (full_point_value_usd * close * number_of_positions))
                    self.broker.sell_future(front_ric, contract_number)
        for ric, position in self.broker.positions['future'].items():
            if position != 0 and will_expire_soon(ric, day=self.day) \
                    and self.market_data.is_trading_day(day=self.day, ric=ric):
                self.broker.expire_future(ric)

    def next_indicators(self):
        pass
