import os
from Global_Variables import path_global
import Data_Clean as DC
import re
import pandas as pd

symbol='BTC'
begin_date="2022-10-01"
end_date= "2023-02-21"

os.chdir(path_global.path_spot() + "//binance//book_snapshot_25//" + symbol)
date_range = DC.get_date_range(begin_date, end_date)
example_filename = os.listdir()[0]
match = re.search(r"\d{4}-\d{2}-\d{2}", example_filename)
before, after = example_filename[:match.start()], example_filename[match.end():]
first_date_file = re.findall(r"\d{4}-\d{2}-\d{2}", example_filename)[0]
date = first_date_file
target = date
target_result = pd.DataFrame()
target_file_name = before + target + after
target_result = pd.concat([target_result, pd.read_csv(target_file_name)])
target_result['time'] = target_result['timestamp'].apply(lambda x: DC.TimeStamp_to_NormalTime(x))
target_result['num'] = target_result.index

a = list(target_result[['time','num']].groupby('time').apply(lambda x : x.iloc[-1])['num'])




