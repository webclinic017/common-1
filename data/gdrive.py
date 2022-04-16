import os
from datetime import datetime
from gspread_pandas import conf, Spread
from gspread_pandas.client import Client
import numpy as np

script_path = os.path.abspath(os.path.dirname(__file__))
conf_dir = os.path.join(script_path, '..', 'common', 'utils')
if os.path.exists(conf_dir):
    config = conf.get_config(
        conf_dir=conf_dir, file_name='google_secret.json')
    gdrive_client = Client(config=config)


CURRENCY = 'Currency'
DATE = 'Date'
RIC = 'Ric'
STEM = 'Stem'
TYPE = 'Type'


def get_cash(spread, sheet='Cash'):
    spread = Spread(spread=spread, sheet=sheet, config=config)
    df = spread.sheet_to_df(index=None)
    return datetime.strptime(df.Date.iloc[-1], '%Y-%m-%d').date(), np.float(df.Cash.iloc[-1])


def get_executions(spread):
    spread = Spread(spread=spread, sheet='Executions', config=config)
    df = spread.sheet_to_df(index=None)
    for column in df.columns:
        if column in [CURRENCY, DATE, RIC, STEM, TYPE]:
            continue
        df.loc[:, column] = df.loc[:, column].apply(
            lambda x: float(x) if x != '' else 0)
    return df


def get_returns(spread):
    spread = Spread(spread=spread, sheet='Returns', config=config)
    return spread.sheet_to_df(index=None)


def get_positions(spread, sheet='All Positions'):
    spread = Spread(spread=spread, sheet=sheet, config=config)
    df = spread.sheet_to_df(index=None)
    for column in df.columns:
        if column == DATE:
            continue
        df.loc[:, column] = df.loc[:, column].apply(
            lambda x: float(x) if x != '' else 0)
    return df


def get_table(spread, sheet):
    spread = Spread(spread=spread, sheet=sheet, config=config)
    return spread.sheet_to_df(index=None)


def list_files():
    return gdrive_client.list_spreadsheet_files()


def save_table(df, spread, sheet, index=False):
    spread = Spread(spread=spread, config=config)
    spread.df_to_sheet(
        df, index=index, sheet=sheet, start='A1', replace=True)


def list_worksheets(spread):
    spread = gdrive_client.open(spread)
    return [ws.title for ws in spread.worksheets()]


if __name__ == '__main__':
    print(list_files())
    print(get_positions('Harvester', 'All Positions'))
