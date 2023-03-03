import pandas as pd
import datetime as dt
import os
import Data_Clean as DC
from Global_Variables import path_global
import re
import multiprocessing as mp

Path = path_global.path_spot() + "//binance//book_snapshot_25"
os.chdir(Path + "//" + "ADA")
date_range = [re.findall(r"\d{4}-\d{2}-\d{2}", file)[0] for file in os.listdir()]
match = re.search(r"\d{4}-\d{2}-\d{2}", os.listdir()[0])
before, after = os.listdir()[0][:match.start()], os.listdir()[0][match.end():]

def process_date(date):
    target_file = before + date + after
    data = pd.read_csv(target_file)
    data['time'] = data['timestamp'].apply(lambda x: DC.TimeStamp_to_NormalTime(x))
    data = data.iloc[:, 4:]
    data['spread'] = data['asks[0].price'] - data['bids[0].price']
    data = data.groupby('time').apply(lambda x: x.iloc[-1])
    return data
