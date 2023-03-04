import os
import re
import time
import numpy as np
import pandas as pd
import Data_Clean as DC
from Global_Variables import path_global
from functools import lru_cache
from tqdm import tqdm

Path = path_global.path_spot() + "//binance//book_snapshot_25"

@lru_cache()
def process_data(currency, date, before, after):
    os.chdir(Path + "//" + currency)
    target_file = before + date + after
    data = pd.read_csv(target_file)
    data['second_timestamp'] = data['timestamp'] // 1000000
    data['middle_price'] = (data['asks[0].price'] + data['bids[0].price'])/2

    data['second_timestamp'] = pd.to_datetime(data['second_timestamp'], unit='s')
    data = data.set_index('second_timestamp')
    data = data.resample('1S').last()
    data = data[['middle_price']]
    all_seconds = pd.date_range(start=data.index.min(), end=data.index.max(), freq='1s')
    data = data.reindex(all_seconds).fillna(method="ffill")
    return data