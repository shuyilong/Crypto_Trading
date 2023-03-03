import os
import re
import pandas as pd
import Data_Clean as DC
from Global_Variables import path_global

Path = path_global.path_spot() + "//binance//book_snapshot_25"
currency = os.listdir(Path)[0]
os.chdir(Path + "//" + currency)
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