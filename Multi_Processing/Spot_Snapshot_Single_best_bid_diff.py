import os
import re
import pandas as pd
import Data_Clean as DC
from Global_Variables import path_global
from functools import lru_cache
from tqdm.notebook import tqdm_notebook

begin_date = "2022-10-01"
end_date = "2023-02-21"
Path = path_global.path_spot() + "//binance//book_snapshot_25"


@lru_cache()
def process_data(date, symbol, period):
    os.chdir(Path + "//" + symbol)
    match = re.search(r"\d{4}-\d{2}-\d{2}", os.listdir()[0])
    before, after = os.listdir()[0][:match.start()], os.listdir()[0][match.end():]

    time_range = DC.time_interval(date + " 00:00:00", date + " 23:59:59", 1)[:200]
    date_results = []

    if date == begin_date:
        file_read = [date, DC.Date_Addtion(date, "day", 1)]
    elif date == end_date:
        file_read = [DC.Date_Addtion(date, "day", -1), date]
    else:
        file_read = [DC.Date_Addtion(date, "day", -1), date, DC.Date_Addtion(date, "day", 1)]

    file = pd.concat([pd.read_csv(before + target + after) for target in file_read])
    file['time'] = file['timestamp'].apply(lambda x: DC.TimeStamp_to_NormalTime(x))

    for second in tqdm_notebook(time_range, desc='Processing data'):
        calculation_time = DC.time_interval(DC.Date_Addtion(second, "second", -period), \
                                            second, 1)
        calculation_data = file[file['time'].isin(calculation_time)]
        date_results.append((calculation_data['bids[0].price'].iloc[-1] - calculation_data['bids[0].price'].iloc[0]) / \
                            calculation_data['bids[0].price'].iloc[0])
    return pd.DataFrame({'time': time_range, symbol: date_results})