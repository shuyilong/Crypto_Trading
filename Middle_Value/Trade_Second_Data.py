import os
import re
import pandas as pd
import Data_Clean as DC
from Global_Variables import path

def trade_second_data():
    ###############################################################################
    ### This function is for calculating trade second data;
    ###############################################################################
    path = path.path_spot + "//binance//trades"
    currency_list = os.listdir(path)
    for currency in currency_list:
        print(currency)
        os.chdir(path + "//" + currency)
        date_range = [re.findall(r"\d{4}-\d{2}-\d{2}", file)[0] for file in os.listdir()]
        match = re.search(r"\d{4}-\d{2}-\d{2}", os.listdir()[0])
        before, after = os.listdir()[0][:match.start()], os.listdir()[0][match.end():]
        Second_Data = pd.DataFrame()
        for date in date_range:
            print(date)
            target_file = before + date + after
            data = pd.read_csv(target_file)
            data['time'] = data['timestamp'].apply(lambda x: DC.TimeStamp_to_NormalTime(x))
            time_range = DC.time_interval(data['time'].iloc[0], data['time'].iloc[-1], 1)
            data = data[data['time'].isin(time_range)]
            data = data.groupby('time').apply(lambda x: x.iloc[-1])
            Second_Data = pd.concat([Second_Data, data])

        Second_Data.index = range(len(Second_Data))
        Middle_path = "E://Backtest Platform//Middle Value"
        os.chdir(Middle_path)
        Second_Data.to_csv(currency + "_Second_Data.csv")
