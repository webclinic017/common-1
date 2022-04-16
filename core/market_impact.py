from datetime import date, datetime, time, timedelta
import time as tm

import numpy as np
import pandas as pd
from tqdm import tqdm

from ..constants import COIN_TYPE, FUTURE_TYPE, STOCK_TYPE
from ..data.database import is_outright, json_data_to_df, ric_to_stem
from ..data.eikon import get_timeseries
from ..constants import FUTURES
from ..utils.dates import is_weekend

DEFAULT_MARKET_IMPACT = 5e-4
OUTRIGHT = 'Outright'
SPREAD = 'Spread'


class MarketImpact:

    def __init__(self, 
            start_date=date.today() - timedelta(days=30), 
            end_date=date.today() - timedelta(days=1),
            mode='relative', instrument_type=FUTURE_TYPE):
        self.cache = {
            COIN_TYPE: {
                'BTC': 0,
                'ETH': 0,
            },
            OUTRIGHT: {
                'AD': 0.00013831258644536604,
                'AEX': 0.00012733176290824488,
                'BAX': 1e-4,
                'BO': 0.0001394700139469851,
                'BP': 0.00014776505356484115,
                'BTC': 0.001,  # guessed figure, to compute
                'C': 0.0007668711656441118,
                'CC': 0.0004154549231407678,
                'CD': 6.374298827127589e-05,
                'CGB': 6.51041666668295e-05,
                'CL': 0.00026723677177975524,
                'CT': 0.00287578033286895,
                'CTF': 4.9227133996243566e-05,
                'CUS': 0.0005,
                'EBL': 5.6827868386566394e-05,
                'EBM': 7.424456158577897e-05,
                'EBS': 4.462094507151626e-05,
                'ED1': 4.558716265501417e-05,
                'ES': 7.955449482888177e-05,
                'FC': 0.000302023557837483,
                'FCH': 0.00020435271278218536,
                'FCE': 0.00015248551387614384,
                'FDX': 6.487187804093963e-05,
                'FEI': 4.9780963759404884e-05,
                'FES': 9.930486593834331e-05,
                'FFI': 8.576329331044796e-05,
                'FLG': 7.278550112821414e-05,
                'FSMI': 8.360504974502803e-05,
                'FSS': 5.0060072086521856e-05,
                'FYG': DEFAULT_MARKET_IMPACT,
                'GC': 0.0001538761408401701,
                'HCE': 0.00019621308741291088,
                'HG': 0.00033277870216297245,
                'HO': 0.00024170157911695966,
                'HSI': 8.089960359192183e-05,
                'HTE': 0.001,  # guessed figure, to compute
                'IFS': 0.00021413276231263545,
                'JGB': 6.585879873544087e-05,
                'JRB': 0.0005,
                'JRU': 0.009669481905011074,
                'JY': 5.6934639034444956e-05,
                'KC': 0.0012442969722108455,
                'KW': 0.0006590509666080102,
                'LB': 0.00417454362117602,
                'LC': 0.00024050024050015217,
                'LCO': 0.00026555722758248024,
                'LGO': 0.000779423226812126,
                'LH': 0.00048076923076911804,
                'MBT': 0.001,  # guessed figure, to compute
                'MFX': 0.0002868617326448053,
                'MGC': 0.0001026272577997478,
                'NE': 7.250054375407267e-05,
                'NG': 0.0003120611639881865,
                'NK': 0.00016949152542378165,
                'NOKA': 0.0002608695652173143,
                'NQ': 4.732159757714349e-05,
                'O': 0.007407407407407307,
                'OJ': 0.0034704184704184033,
                'OMXS30': 0.00011855364552459946,
                'PA': 0.00327423538884406,
                'PL': 0.0005521201413427601,
                'RA2': 0.0006565988181221893,
                'RB': 0.00027586206896579313,
                'RF': 9.258401999812094e-05,
                'RP': DEFAULT_MARKET_IMPACT,
                'RR': 0.0020945043743445524,
                'RS': 0.00038940809968823764,
                'RY': 7.742335088245511e-05,
                'S': 0.00027570995312919955,
                'SAF': 0.00022644927536230597,
                'SB': 0.0005,  # guessed figure, to compute
                'SEK': 0.0001749475157453162,
                'SF': 9.292816652717306e-05,
                'SI': 0.0005418096442115772,
                'SM': 0.00034352456200603676,
                'SNI': 0.0003198099415204769,
                'SP': DEFAULT_MARKET_IMPACT,
                'SPB': 0.0003305785123965954,
                'SRS': 0.0018105009052504784,
                'STW': 0.0003957261574991655,
                'STXE': DEFAULT_MARKET_IMPACT,
                'SXF': 0.00010450412791307429,
                'SXE': 0.00012350253180182236,
                'SXX': 0.0006624710168929848,
                'SZN': 0.00019912385503784158,
                'TY': 0.0001,  # guessed figure, to compute
                'W': 0.0004909180166912108,
                'YAP': 0.0008341675008340932,
                'YBA': 0.00010013016921983642,
                'YTC': 5.0469365095562324e-05,
                'YTT': 5.0155481994051954e-05,
                'ZF': 6.215813028354056e-05,
                'ZN': 0.0001123343068973881,
                'ZT': 3.538445207174057e-05,
            },
            SPREAD: {
                'BO': 0.02,
                'C': 0.25,
                'CC': 1,
                'CL': 0.00026,
                'CT': 0,
                'FC': 0.05,
                'HG': 0.00033,
                'HO': 0.00024,
                'KC': 0,
                'LB': 0.004,
                'LC': 0.00024,
                'LH': 0.00048,
                'O': 0,
                'OJ': 0.0035,
                'RR': 0.0021,
                'SB': 0.01,
                'SI': 0.0005,
                'W': 0.25
            },
            STOCK_TYPE: {
                'MICP.PA': 0,
                'LEGD.PA': 0,
                'BNPP.PA': 0,
                'MT.AS': 0,
                'PUBP.PA': 0,
                'BOUY.PA': 0,
                'VIE.PA': 0,
                'ATOS.PA': 0,
                'PRTP.PA': 0,
                'LVMH.PA': 0,
                'AXAF.PA': 0,
                'ALSO.PA': 0,
                'CAGR.PA': 0,
                'SOGN.PA': 0,
                'SCHN.PA': 0,
                'SASY.PA': 0,
                'WLN.PA': 0,
                'RENA.PA': 0,
                'ENGIE.PA': 0,
                'TEPRF.PA': 0,
                'SAF.PA': 0,
                'HRMS.PA': 0,
                'DAST.PA': 0,
                'TOTF.PA': 0,
                'URW.AS': 0,
                'ESLX.PA': 0,
                'PERP.PA': 0,
                'VIV.PA': 0,
                'SGEF.PA': 0,
                'ORAN.PA': 0,
                'CAPP.PA': 0,
                'AIR.PA': 0,
                'AIRP.PA': 0,
                'STLA.PA': 0,
                'STM.PA': 0,
                'OREP.PA': 0,
                'SGOB.PA': 0,
                'CARR.PA': 0,
                'TCFP.PA': 0,
                'DANO.PA': 0
            }
        }
        self.end_date = end_date
        self.start_date = start_date
        self.mode = mode
        self.instrument_type = instrument_type

    def get(self, stem=None, ric=None):
        if self.instrument_type == STOCK_TYPE:
            default_stock_fee = 5e-4
            return default_stock_fee
        elif self.instrument_type == COIN_TYPE:
            return self.cache[self.instrument_type].get(ric, 0.01)
        else:
            instrument_type = OUTRIGHT if ric is None or is_outright(
                ric) else SPREAD
        if stem is None:
            stem = ric_to_stem(ric)
        if stem not in self.cache[instrument_type]:
            frames = []
            delta = self.end_date - self.start_date
            for i in tqdm(range(delta.days + 1)):
                day = self.start_date + timedelta(days=i)
                if is_weekend(day):
                    continue
                for hour in range(24):
                    start_time = time(hour, 0)
                    end_time = time(hour, 59, 59, 999999)
                    start_date = datetime.combine(day, start_time)
                    end_date = datetime.combine(day, end_time)
                    if not ric:
                        reuters_stem = FUTURES[stem]['Stem']['Reuters']
                        ric = f'{reuters_stem}c1'
                    response = get_timeseries(rics=ric,
                                              fields=['BID', 'ASK'],
                                              start_date=start_date.isoformat(),
                                              end_date=end_date.isoformat(),
                                              interval='tas')
                    frame = json_data_to_df(response['data'])
                    tm.sleep(1)
                    frames.append(frame)
            dfm = pd.concat(frames)
            spread = []
            if self.mode == 'relative':
                spread = dfm.ASK / dfm.BID - 1
            elif self.mode == 'absolute':
                spread = dfm.ASK - dfm.BID
            self.cache[instrument_type][stem] = np.nanquantile(
                spread, 0.5) if not np.all(pd.isnull(spread.values)) else DEFAULT_MARKET_IMPACT
        market_impact = self.cache[instrument_type][stem]
        return market_impact
