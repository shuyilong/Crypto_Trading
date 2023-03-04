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
    files = sorted(os.listdir())
    match = re.search(r"\d{4}-\d{2}-\d{2}", files[0])
    before, after = files[0][:match.start()], files[0][match.end():]
    Date_Range = DC.get_date_range(begin_date, end_date)

    cpu_num = 32
    Final_Result_List = []
    for i in range(len(Date_Range) // cpu_num + (len(Date_Range) % cpu_num > 0)):
        Final_Result = pd.DataFrame()
        date_range = Date_Range[i*cpu_num : (1+i) * cpu_num]
        pool = mp.Pool(processes= cpu_num)
        results = [pool.apply_async(Spot_Snapshot_Single_best_bid_diff.process_data, args=(date, symbol, period)) \
                   for date in date_range]
        for result in tqdm(results):
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

###################################################################################################
def best_ask_diff(symbol, period, begin_date="2022-10-01", end_date= "2023-02-21"):
    Path = path_global.path_spot() + "//binance//book_snapshot_25"
    os.chdir(Path + "//"+symbol)
    files = sorted(os.listdir())
    match = re.search(r"\d{4}-\d{2}-\d{2}", files[0])
    before, after = files[0][:match.start()], files[0][match.end():]
    Date_Range = DC.get_date_range(begin_date, end_date)

    cpu_num = 32
    Final_Result_List = []
    for i in range(len(Date_Range) // cpu_num + (len(Date_Range) % cpu_num > 0)):
        Final_Result = pd.DataFrame()
        date_range = Date_Range[i*cpu_num : (1+i) * cpu_num]
        pool = mp.Pool(processes= cpu_num)
        results = [pool.apply_async(Spot_Snapshot_Single_best_bid_diff.process_data, args=(date, symbol, period)) \
                   for date in date_range]
        for result in tqdm(results):
            Final_Result = pd.concat([Final_Result, result.get()])
        Final_Result_List.append(Final_Result)
        pool.close()
        pool.join()

    Final_Result_List = pd.concat(Final_Result_List)
    Final_Result_List.index = range(len(Final_Result_List))
    file_path = path_global.path_middle() + "//Features"
    if not os.path.exists(file_path + '//best_ask_diff'):
        os.makedirs(file_path + '//best_ask_diff')
    os.chdir(file_path + '//best_ask_diff')
    Final_Result_List.to_csv(symbol + "_" + str(period) + ".csv")
