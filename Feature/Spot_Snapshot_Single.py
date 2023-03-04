import os
from Global_Variables import path_global
import Data_Clean as DC
import re
import pandas as pd
import time
import multiprocessing as mp
from Multi_Processing import Spot_Snapshot_Single_best_bid_diff
from tqdm import tqdm
import math

def best_bid_diff(symbol, period, begin_date="2022-10-01", end_date= "2023-02-21"):
    Path = path_global.path_spot() + "//binance//book_snapshot_25"
    os.chdir(Path + "//"+symbol)
    match = re.search(r"\d{4}-\d{2}-\d{2}", os.listdir()[0])
    before, after = os.listdir()[0][:match.start()], os.listdir()[0][match.end():]
    Date_Range = DC.get_date_range(begin_date, end_date)

    cpu_num = mp.cpu_count()
    Final_Result_List = []
    for i in range(math.ceil(len(Date_Range)//cpu_num)):
        Final_Result = pd.DataFrame()
        date_range = Date_Range[i*cpu_num : (1+i) * cpu_num]
        pool = mp.Pool(processes=mp.cpu_count())
        results = [pool.apply_async(Spot_Snapshot_Single_best_bid_diff.process_data, args=(date, symbol, period)) \
                   for date in date_range]
        for result in results:
            Final_Result = pd.concat([Final_Result, result.get()])
        Final_Result_List.append(Final_Result)
        pool.close()
        pool.join()

    Final_Result_List = pd.concat(Final_Result_List)
    Final_Result_List.index = range(len(Final_Result_List))
    file_path = path_global.path_middle() + "//Features"
    if not os.path.exists(file_path + '//best_bid_diff'):
        os.makedirs(file_path + '//best_bid_diff')
    os.chdir(file_path + '//best_bid_diff')
    Final_Result_List.to_csv(symbol + "_" + str(period) + ".csv")

