import os
import re
import pandas as pd
import Data_Clean as DC
import Global_Variables as GV
from functools import lru_cache

begin_date = GV.begin_date()
end_date = GV.end_date()
Path = GV.path_spot() + "//binance//book_snapshot_25"


@lru_cache()
def process_data(date, symbol, period, n, data_type, direction):
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

    if direction == "bid":
        price_list = [f"bids[{i}].price" for i in range(n+1)]
        amount_list = [f"bids[{i}].amount" for i in range(n+1)]
    elif direction == "ask":
        price_list = [f"asks[{i}].price" for i in range(n+1)]
        amount_list = [f"asks[{i}].amount" for i in range(n+1)]
    else:
        raise ValueError("direction should be ask or bid")

    file['n_depth'] = 0
    for i in range(n+1):
        file['n_depth'] = file['n_depth'] + file[price_list[i]] * file[amount_list[i]]

    file = file[['n_depth']].groupby(pd.Grouper(freq='1s')).mean()
    all_seconds = pd.date_range(start=file.index.min(), end=file.index.max(), freq='1s')
    file = file.reindex(all_seconds).fillna(method="ffill")

    rolling = file['n_depth'].rolling(window=period, min_periods=1)
    func_dict = {"mean": rolling.mean, "median": rolling.median, "max": rolling.max, \
                 "min": rolling.min, "std": rolling.std, "sum": rolling.sum}
    file['feature'] = func_dict[data_type]().shift(1)
    file['second_timestamp'] = pd.DatetimeIndex(file.index).astype(int) // 10 ** 9

    start_time = pd.Timestamp(date + ' 00:00:00')
    end_time = pd.Timestamp(date + ' 23:59:59')
    mask = file.index.slice_indexer(start_time, end_time)
    file = file[['second_timestamp','feature']].iloc[mask]
    return file