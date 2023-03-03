import os
from Global_Variables import path_global
import Data_Clean as DC
import re
import pandas as pd
import time

def best_bid_diff(symbol, period, begin_date="2022-10-01", end_date= "2023-02-21"):
    os.chdir(path_global.path_spot() + "//binance//book_snapshot_25//"+symbol)
    date_range = DC.get_date_range(begin_date, end_date)
    example_filename = os.listdir()[0]
    match = re.search(r"\d{4}-\d{2}-\d{2}", example_filename)
    before, after = example_filename[:match.start()], example_filename[match.end():]
    first_date_file = re.findall(r"\d{4}-\d{2}-\d{2}", example_filename)[0]
    for date in date_range:
        if date == first_date_file:
            target_list = [date, DC.Date_Addtion(date,"day", 1)]
            target_result = pd.DataFrame()
            for target in target_list:
                target_file_name = before + target + after
                target_result = pd.concat([target_result, pd.read_csv(target_file_name)])
                target_result['time'] = target_result['timestamp'].apply(lambda x: DC.TimeStamp_to_NormalTime(x))
            time_range = DC.time_interval(date + " 00:00:00", date + " 23:59:59", 1)
            indicator_result = []


            start = time.time()
            k=1
            for second in time_range:
                calculation_time = DC.time_interval(DC.Date_Addtion(second,"second",-period),\
                                                    second, 1)
                calculation_table = target_result[target_result['time'].isin(calculation_time)]
                indicator_result.append((calculation_table['bids[0].price'].iloc[-1] - \
                        calculation_table['bids[0].price'].iloc[0])/calculation_table['bids[0].price'].iloc[0])
                k+=1
                if k%100 == 1:
                    print(time.time()-start)
                    print(k)



