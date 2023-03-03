import os
from Global_Variables import path_global
import Data_Clean as DC
import re
import pandas as pd
import time
import multiprocessing as mp
from Multi_Processing import Spot_Snapshot_Single_best_bid_diff
from tqdm import tqdm

def best_bid_diff(symbol, period, begin_date="2022-10-01", end_date= "2023-02-21"):
    Path = path_global.path_spot() + "//binance//book_snapshot_25"
    os.chdir(Path + "//"+symbol)
    match = re.search(r"\d{4}-\d{2}-\d{2}", os.listdir()[0])
    before, after = os.listdir()[0][:match.start()], os.listdir()[0][match.end():]
    date_range = DC.get_date_range(begin_date, end_date)
    Final_Result = pd.DataFrame()

    pool = mp.Pool(processes=mp.cpu_count())
    results = []
    for date in tqdm_notebook(date_range, desc='Processing data'):
        result = pool.apply_async(Spot_Snapshot_Single_best_bid_diff.process_data, args=(date, symbol, period))
        results.append(result)

    for result in tqdm(results, total=len(date_range)):
        Final_Result = pd.concat([Final_Result, result.get()])
    Final_Result.index = range(len(Final_Result))

    file_path = path_global.path_middle() + "//Features"
    if not os.path.exists(file_path + '//best_bid_diff'):
        os.makedirs(file_path + '//best_bid_diff')
    os.chdir(file_path + '//best_bid_diff')
    Final_Result.to_csv(symbol + "_" + str(period) +".csv")
