from datetime import date
import json
import os


BACKTEST = 'backtest'
DOWNLOAD = 'download'
EXECUTE = 'execute'
LIVE = 'live'
SCREEN = 'screen'

ALPHA_VANTAGE = 'alpha vantage'
COINAPI = 'coinapi'
YAHOO = 'yahoo'

HARVESTER = 'harvester'
MOMENTUM = 'momentum'
TURTLE = 'turtle'

CREATE_ORDER = 'create order'
GET_EXECUTION = 'get execution'
GET_ORDER = 'get order'
GET_POSITION = 'get position'

LIVE_REFERENCE_DATE = date(2019, 5, 17)


# Futures letter codes
LETTERS = ['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z']

COIN_TYPE = 'coin'
STOCK_TYPE = 'stock'
FUTURE_TYPE = 'future'




EUROPEAN_INDICES = [
    '0#.FCHI',
    '0#.GDAXI',
    '0#.FTSE',
    '0#.IBEX',
    '0#.SSMI',
    '0#.AEX',
    '0#.FTMIB',
    '0#.BFX',
]

AMERICAN_INDICES = [
    '0#.SPX',
    '0#.NYA',
    '0#.NDX',
    '0#.RUA',
]

SP500_INDEX = '0#.SPX'

CRYPTOCURRENCIES = [
    'BTC=',
    'ETH=',
    'XRP=',
    'LTC='
]

AMERICAN_STOCKS = 'SCREEN(U(IN(Equity(active,public,primary))), IN(TR.ExchangeMarketIdCode, XASE, XNAS, XNYS))'
ARCA_STOCKS = 'SCREEN(U(IN(Equity(active,public,primary))), IN(TR.ExchangeMarketIdCode, XASE))'
NASDAQ_STOCKS = 'SCREEN(U(IN(Equity(active,public,primary))), IN(TR.ExchangeMarketIdCode, XNAS))'
NYSE_STOCKS = 'SCREEN(U(IN(Equity(active,public,primary))), IN(TR.ExchangeMarketIdCode, XNYS))'


SYMBOL_TO_RIC = {
    'SPY': 'SPY',
    'EWJ': 'EWJ',
    'VNQ': 'VNQ',
    'IEF': 'IEF.O',
    'DBC': 'DBC',
    'VGK': 'VGK',
    'VWO': 'VWO',
    'VNQI': 'VNQI.O',
    'TLT': 'TLT.O',
    'GLD': 'GLD',
    'BTC': 'BTC=',
    'ETH': 'ETH=',
    'XRP': 'XRP=',
    'LTC': 'LTC=',
}


EXCHANGES = {
    'ARCX': 'New York Stock Exchange Archipelago',
    'FMTS': 'MTS France SAS',
    'WBAH': 'Vienna Stock Exchange',
    'XAMS': 'Euronext Amsterdam',
    'XASE': 'American Stock Exchange',
    'XASX': 'Australian Stock Exchange',
    'XBER': 'Borse Berlin',
    'XBKK': 'Stock Exchange of Thailand',
    'XBOM': 'Bombay Stock Exchange',
    'XBRU': 'Euronext Brussels',
    'XETR': 'Xetra Stock Exchange',
    'XFRA': 'Deutsche Boerse AG',
    'XHKG': 'The Stock Exchange of Hong Kong Limited',
    'XIDX': 'Indonesia Stock Exchange',
    'XJAS': 'JASDAQ Securities Exchange',
    'XKAR': 'Karachi Stock Exchange',
    'XKLS': 'Kuala Lumpur Stock Exchange',
    'XKRX': 'Korea Stock Exchange',
    'XLON': 'London Stock Exchange',
    'XNAS': 'NASDAQ',
    'XNGO': 'Nagoya Stock Exchange',
    'XNGS': 'NASDAQ GS',
    'XNYS': 'New York Stock Exchange',
    'XNZE': 'New Zealand Exchange',
    'XOSE': 'Osaka Securities Exchange',
    'XOSL': 'Oslo Stock Exchange',
    'XPAR': 'Euronext Paris',
    'XSES': 'Singapore Exchange Securities Trading Limited',
    'XSHE': 'Shenzhen Stock Exchange',
    'XSHG': 'Shanghai Stock Exchange',
    'XSTC': 'Hochiminh Stock Exchange',
    'XSTO': 'Stockholm Stock Exchange',
    'XSWX': 'Swiss Exchange',
    'XTAI': 'Taiwan Stock Exchange',
    'XTKS': 'Tokyo Stock Exchange',
    'XVTX': 'Six Swiss Exchange',
    'YYYY': 'All German Stock Exchanges',
}

script_path = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(script_path, 'database-futures.json'), 'r') as f:
    FUTURES = json.load(f)

LIBOR_BEFORE_2001 = 6.65125

START_DATE = date(2000, 1, 1)

MAXIMUM_REQUEST_SIZE = 3000
