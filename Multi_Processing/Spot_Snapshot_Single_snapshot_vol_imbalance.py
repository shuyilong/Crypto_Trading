import os
import re
import pandas as pd
import Data_Clean as DC
from Global_Variables import path_global
from functools import lru_cache
import numpy as np

begin_date = path_global.begin_date()
end_date = path_global.end_date()
Path = path_global.path_spot() + "//binance//book_snapshot_25"

@lru_cache()
def process_data(date, symbol, period, n, data_type):
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

    bid_amount = [f"bids[{i}].amount" for i in range(n+1)]
    ask_amount = [f"asks[{i}].amount" for i in range(n+1)]
    file['bid_amount'] = np.sum(file[bid_amount].values, axis=1)
    file['ask_amount'] = np.sum(file[ask_amount].values, axis=1)

    file = file[['bid_amount','ask_amount']].groupby(pd.Grouper(freq='1s')).sum()
    file['vol_imb'] = file['bid_amount'] / (file['ask_amount'] + file['bid_amount'])

    rolling = file['vol_imb'].rolling(window=period, min_periods=1)
    func_dict = {"mean": rolling.mean, "median": rolling.median, "max": rolling.max, \
                 "min": rolling.min, "std": rolling.std, "sum": rolling.sum}
    file['feature'] = func_dict[data_type]().shift(1)
    file['second_timestamp'] = pd.DatetimeIndex(file.index).astype(int) // 10 ** 9

    start_time = pd.Timestamp(date + ' 00:00:00')
    end_time = pd.Timestamp(date + ' 23:59:59')
    mask = file.index.slice_indexer(start_time, end_time)
    file = file[['second_timestamp', 'feature']].iloc[mask]
    return file