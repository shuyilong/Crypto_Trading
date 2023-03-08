import os
import re
import pandas as pd
import Data_Clean as DC
from Global_Variables import path_global
from functools import lru_cache

begin_date = path_global.begin_date()
end_date = path_global.end_date()
Path = path_global.path_spot() + "//binance//trades"


@lru_cache()
def process_data(date, symbol, period):
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

    buy_side = file[file['side'] == 'buy'][['amount']].groupby(pd.Grouper(freq='1s')).count()
    sell_side = file[file['side'] == 'sell'][['amount']].groupby(pd.Grouper(freq='1s')).count()
    all_seconds = pd.date_range(start=file.index.min(), end=file.index.max(), freq='1s')
    buy_side = buy_side.reindex(all_seconds).fillna(0)
    sell_side = sell_side.reindex(all_seconds).fillna(0)

    buy_side = buy_side[['amount']].rolling(window=period, min_periods=1).sum()
    sell_side = sell_side[['amount']].rolling(window=period, min_periods=1).sum()
    result = (buy_side / sell_side).shift(1).rename(columns={'amount': 'feature'})
    result['second_timestamp'] = pd.DatetimeIndex(result.index).astype(int) // 10**9

    start_time = pd.Timestamp(date + ' 00:00:00')
    end_time = pd.Timestamp(date + ' 23:59:59')
    mask = result.index.slice_indexer(start_time, end_time)
    result = result[['second_timestamp','feature']].iloc[mask]
    return result