import os
import pandas as pd
import Data_Clean as DC
from Global_Variables import path_global
import re

def spot_snapshot_second_data():
    Path = path_global.path_spot() + "//binance//book_snapshot_25"
    currency_list = os.listdir(Path)
    for currency in currency_list:
        os.chdir(Path + "//" + currency)
        date_range = [re.findall(r"\d{4}-\d{2}-\d{2}", file)[0] for file in os.listdir()]
        match = re.search(r"\d{4}-\d{2}-\d{2}", os.listdir()[0])
        before, after = os.listdir()[0][:match.start()], os.listdir()[0][match.end():]
        Second_Data = pd.DataFrame()
        for date in date_range:
            target_file = before + date + after
            data = pd.read_csv(target_file)
            data['time'] = data['timestamp'].apply(lambda x: DC.TimeStamp_to_NormalTime(x))
            data = data.iloc[:,4:]
            data['spread'] = data['asks[0].price'] - data['bids[0].price']
            data = data.groupby('time').apply(lambda x: x.iloc[-1])
            Second_Data = pd.concat([Second_Data, data])

        Second_Data.index = range(len(Second_Data))
        file_path = os.chdir(path_global.path_middle() + '//Spot_Snapshot_Second_Data')
        if not os.path.exists(file_path):
            os.makedirs(file_path)
        os.chdir(file_path)
        Second_Data.to_csv(currency + "_Second_Data.csv")





