import os
import re
import time
import numpy as np
import pandas as pd
import Data_Clean as DC
import Global_Variables as GV
from functools import lru_cache
from tqdm import tqdm

begin_date = GV.begin_date()
end_date = GV.end_date()
Path = GV.path_spot() + "//binance//book_snapshot_25"

@lru_cache()
def process_data(date, symbol, long_period, short_period):
    os.chdir(Path + "//" + symbol)
    match = re.search(r"\d{4}-\d{2}-\d{2}", os.listdir()[0])
    before, after = os.listdir()[0][:match.start()], os.listdir()[0][match.end():]

    if date == begin_date:
        file_read = [date, DC.Date_Addtion(date, "day", 1)]
    elif date == end_date:
        file_read = [DC.Date_Addtion(date, "day", -1), date]
    else:
        file_read = [DC.Date_Addtion(date, "day", -1), date, DC.Date_Addtion(date, "day", 1)]

    file = pd.concat([pd.read_csv(before + target + after) for target in file_read])
    file['second_timestamp'] = file['timestamp'] // 1000000
    file['second_timestamp'] = pd.to_datetime(file['second_timestamp'], unit='s')
    file = file.set_index('second_timestamp')

    file['middle_price'] = (file['asks[0].price'] + file['bids[0].price']) / 2
    file = file.resample('1S').last()
    all_seconds = pd.date_range(start=file.index.min(), end=file.index.max(), freq='1s')
    file = file.reindex(all_seconds).fillna(method="ffill")

    file['short_ema'] = file[['middle_price']].rolling(window = short_period, \
                    min_periods=1).apply(lambda x: pd.Series(x).ewm(alpha=0.2).mean().iloc[-1]).shift(1)
    file['long_ema'] = file[['middle_price']].rolling(window=long_period, \
                    min_periods=1).apply(lambda x: pd.Series(x).ewm(alpha=0.2).mean().iloc[-1]).shift(1)
    file['feature'] = 10000 * (file['short_ema'] - file['long_ema'])/file['short_ema']
    file['second_timestamp'] = pd.DatetimeIndex(file.index).astype(int) // 10 ** 9

    start_time = pd.Timestamp(date + ' 00:00:00')
    end_time = pd.Timestamp(date + ' 23:59:59')
    mask = file.index.slice_indexer(start_time, end_time)
    file = file[['second_timestamp', 'feature']].iloc[mask]
    return file