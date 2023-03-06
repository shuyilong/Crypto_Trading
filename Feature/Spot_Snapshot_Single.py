import os
from Global_Variables import path_global
import Data_Clean as DC
import re
import pandas as pd
import time
import multiprocessing as mp
from Multi_Processing import Spot_Snapshot_Single_best_bid_diff, Spot_Snapshot_Single_best_ask_diff, \
    Spot_Snapshot_Single_ask_n_depth, Spot_Snapshot_Single_bid_n_depth
from tqdm import tqdm
import math

def best_bid_diff(symbol, period, begin_date=path_global.begin_date(), end_date= path_global.end_date()):
    ###############################################################################
    ### This function is for calculating best bid diff of given currency;
    ### INPUT : 1) symbol, e.g: "BTC"
    ###         2) period, period of return calculation, in seconds
    ###         3) begin_date, default in "2022-10-01"
    ###         4) end, default in "2022-10-01"
    ###############################################################################
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
def best_ask_diff(symbol, period, begin_date=path_global.begin_date(), end_date= path_global.end_date()):
    ###############################################################################
    ### This function is for calculating best ask diff of given currency;
    ### INPUT : 1) symbol, e.g: "BTC"
    ###         2) period, period of return calculation, in seconds
    ###         3) begin_date, default in "2022-10-01"
    ###         4) end, default in "2022-10-01"
    ###############################################################################
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

###################################################################################################
def bid_n_depth(symbol, period, n, data_type, begin_date=path_global.begin_date(), end_date= path_global.end_date()):
    ###############################################################################
    ### This function is for calculating the top n bid depth of given currency;
    ### INPUT : 1) symbol, e.g: "BTC"
    ###         2) period, period of return calculation, in seconds
    ###         3) n, You want to study the price of the nth block.
    ###         4) data_type, choose from "mean","max","min","std","median","sum"
    ###         5) begin_date, default in "2022-10-01"
    ###         6) end, default in "2022-10-01"
    ###############################################################################
    Path = path_global.path_spot() + "//binance//book_snapshot_25"
    os.chdir(Path + "//" + symbol)
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
        results = [pool.apply_async(Spot_Snapshot_Single_bid_n_depth.process_data, args=(date, symbol, period, n, data_type)) \
                   for date in date_range]
        for result in tqdm(results):
            Final_Result = pd.concat([Final_Result, result.get()])
        Final_Result_List.append(Final_Result)
        pool.close()
        pool.join()

    Final_Result_List = pd.concat(Final_Result_List)
    Final_Result_List.index = range(len(Final_Result_List))
    file_path = path_global.path_middle() + "//Features"
    if not os.path.exists(file_path + '//bid_'+str(n)+'_depth_'+data_type):
        os.makedirs(file_path + '//bid_'+str(n)+'_depth_'+data_type)
    os.chdir(file_path + '//bid_'+str(n)+'_depth_'+data_type)
    Final_Result_List.to_csv(symbol + "_" + str(period) + ".csv")

###################################################################################################
def ask_n_depth(symbol, period, n, data_type, begin_date=path_global.begin_date(), end_date= path_global.end_date()):
    ###############################################################################
    ### This function is for calculating the top n bid depth of given currency;
    ### INPUT : 1) symbol, e.g: "BTC"
    ###         2) period, period of return calculation, in seconds
    ###         3) n, You want to study the price of the nth block.
    ###         4) data_type, choose from "mean","max","min","std","median","sum"
    ###         5) begin_date, default in "2022-10-01"
    ###         6) end, default in "2022-10-01"
    ###############################################################################
    Path = path_global.path_spot() + "//binance//book_snapshot_25"
    os.chdir(Path + "//" + symbol)
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
        results = [pool.apply_async(Spot_Snapshot_Single_ask_n_depth.process_data, args=(date, symbol, period, n, data_type)) \
                   for date in date_range]
        for result in tqdm(results):
            Final_Result = pd.concat([Final_Result, result.get()])
        Final_Result_List.append(Final_Result)
        pool.close()
        pool.join()

    Final_Result_List = pd.concat(Final_Result_List)
    Final_Result_List.index = range(len(Final_Result_List))
    file_path = path_global.path_middle() + "//Features"
    if not os.path.exists(file_path + '//ask_'+str(n)+'_depth_'+data_type):
        os.makedirs(file_path + '//ask_'+str(n)+'_depth_'+data_type)
    os.chdir(file_path + '//ask_'+str(n)+'_depth_'+data_type)
    Final_Result_List.to_csv(symbol + "_" + str(period) + ".csv")

###################################################################################################
def window_return(symbol, period, data_type, begin_date=path_global.begin_date(), end_date= path_global.end_date()):
    ###############################################################################
    ### This function is for calculating the statistic of return;
    ### INPUT : 1) symbol, e.g: "BTC"
    ###         2) period, period of return calculation, in seconds
    ###         3) data_type, choose from "mean","max","min","std","median","sum"
    ###         4) begin_date, default in "2022-10-01"
    ###         5) end, default in "2022-10-01"
    ###############################################################################
    Path = path_global.path_middle_second_data()
    os.chdir(Path)
    file = pd.read_csv(symbol + "_Second_data.csv")
    file['second_timestamp'] = pd.to_datetime(file['second_timestamp'], unit='s')
    file = file.set_index('second_timestamp')
    all_seconds = pd.date_range(start=file.index.min(), end=file.index.max(), freq='1s')
    file = file.reindex(all_seconds).fillna(method="ffill")
    file['ret'] = (file['middle_price'] / file['middle_price'].shift(1) - 1) * 10000

    rolling = file['ret'].rolling(window=period, min_periods=1)
    func_dict = {"mean": rolling.mean, "median": rolling.median, "max": rolling.max, \
                 "min": rolling.min, "std": rolling.std, "sum": rolling.sum}
    file['feature'] = func_dict[data_type]().shift(1)
    file['second_timestamp'] = pd.DatetimeIndex(file.index).astype(int) // 10 ** 9

    start_time = pd.Timestamp(begin_date + ' 00:00:00')
    end_time = pd.Timestamp(end_date + ' 23:59:59')
    mask = file.index.slice_indexer(start_time, end_time)
    file = file[['second_timestamp', 'feature']].iloc[mask]
    file.index = range(len(file))
    file_path = path_global.path_middle() + "//Features"
    if not os.path.exists(file_path + '//window_return_'+data_type):
        os.makedirs(file_path + '//window_return_'+data_type)
    os.chdir(file_path + '//window_return_'+data_type)
    file.to_csv(symbol + "_" + str(period) + ".csv")

###################################################################################################
from Multi_Processing import Spot_Snapshot_Single_spread_return
def spread_return(symbol, period, data_type, n, begin_date=path_global.begin_date(), end_date= path_global.end_date()):
    ###############################################################################
    ### This function is for calculating the statistic of spread return;
    ### INPUT : 1) symbol, e.g: "BTC"
    ###         2) period, period of return calculation, in seconds
    ###         3) data_type, choose from "mean","max","min","std","median","sum"
    ###         4) n, n-th ask and bid, from 0 to 24
    ###         5) begin_date, default in "2022-10-01"
    ###         6) end, default in "2022-10-01"
    ###############################################################################
    Path = path_global.path_spot() + "//binance//book_snapshot_25"
    os.chdir(Path + "//" + symbol)
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
        results = [pool.apply_async(Spot_Snapshot_Single_spread_return.process_data, args=(date, symbol, period, n, data_type)) \
                   for date in date_range]
        for result in tqdm(results):
            Final_Result = pd.concat([Final_Result, result.get()])
        Final_Result_List.append(Final_Result)
        pool.close()
        pool.join()

    Final_Result_List = pd.concat(Final_Result_List)
    Final_Result_List.index = range(len(Final_Result_List))
    file_path = path_global.path_middle() + "//Features"
    if not os.path.exists(file_path + '//spread_return_'+data_type):
        os.makedirs(file_path + '//spread_return_'+data_type)
    os.chdir(file_path + '//spread_return_'+data_type)
    Final_Result_List.to_csv(symbol + "_" + str(period) + ".csv")

###################################################################################################
from Multi_Processing import Spot_Snapshot_Single_snapshot_derivative
def snapshot_derivative(symbol, period, data_type, target_data, n, begin_date=path_global.begin_date(), end_date= path_global.end_date()):
    ###############################################################################
    ### This function is for calculating the statistic of spread return;
    ### INPUT : 1) symbol, e.g: "BTC"
    ###         2) period, period of return calculation, in seconds
    ###         3) data_type, choose from "mean","max","min","std","median","sum"
    ###         4) target_data, choose from "ask_price","bid_price","middle_price",
    ###                                     "ask_amount","bid_amound"
    ###         5) n, n-th ask and bid, from 0 to 24
    ###         6) begin_date, default in "2022-10-01"
    ###         7) end, default in "2022-10-01"
    ###############################################################################
    Path = path_global.path_spot() + "//binance//book_snapshot_25"
    os.chdir(Path + "//" + symbol)
    files = sorted(os.listdir())
    match = re.search(r"\d{4}-\d{2}-\d{2}", files[0])
    before, after = files[0][:match.start()], files[0][match.end():]
    Date_Range = DC.get_date_range(begin_date, end_date)

    cpu_num = 32
    Final_Result_List = []
    for i in range(len(Date_Range) // cpu_num + (len(Date_Range) % cpu_num > 0)):
        Final_Result = pd.DataFrame()
        date_range = Date_Range[i * cpu_num: (1 + i) * cpu_num]
        pool = mp.Pool(processes=cpu_num)
        results = [
            pool.apply_async(Spot_Snapshot_Single_snapshot_derivative.process_data, \
                    args=(date, symbol, period, n, data_type, target_data)) for date in date_range]
        for result in tqdm(results):
            Final_Result = pd.concat([Final_Result, result.get()])
        Final_Result_List.append(Final_Result)
        pool.close()
        pool.join()

    Final_Result_List = pd.concat(Final_Result_List)
    Final_Result_List.index = range(len(Final_Result_List))
    file_path = path_global.path_middle() + "//Features"
    if not os.path.exists(file_path + '//snapshot_derivative//' + target_data + '_' + data_type):
        os.makedirs(file_path + '//snapshot_derivative//' + target_data + '_' + data_type)
    os.chdir(file_path + '//snapshot_derivative//' + target_data + '_' + data_type)
    Final_Result_List.to_csv(symbol + "_" + str(period) + ".csv")

###################################################################################################
from Multi_Processing import Spot_Snapshot_Single_semi_std
def semi_std(symbol, period, direction, begin_date=path_global.begin_date(), end_date= path_global.end_date()):
    ###############################################################################
    ### This function is for calculating the semi-std of middle price;
    ### INPUT : 1) symbol, e.g: "BTC"
    ###         2) period, period of return calculation, in seconds
    ###         3) direction, choose from "positive","negative"
    ###         4) begin_date, default in "2022-10-01"
    ###         5) end, default in "2022-10-01"
    ###############################################################################
    Path = path_global.path_spot() + "//binance//book_snapshot_25"
    os.chdir(Path + "//" + symbol)
    files = sorted(os.listdir())
    match = re.search(r"\d{4}-\d{2}-\d{2}", files[0])
    before, after = files[0][:match.start()], files[0][match.end():]
    Date_Range = DC.get_date_range(begin_date, end_date)

    cpu_num = 32
    Final_Result_List = []
    for i in range(len(Date_Range) // cpu_num + (len(Date_Range) % cpu_num > 0)):
        Final_Result = pd.DataFrame()
        date_range = Date_Range[i * cpu_num: (1 + i) * cpu_num]
        pool = mp.Pool(processes=cpu_num)
        results = [
            pool.apply_async(Spot_Snapshot_Single_semi_std.process_data, args=(date, symbol, period, direction)) \
            for date in date_range]
        for result in tqdm(results):
            Final_Result = pd.concat([Final_Result, result.get()])
        Final_Result_List.append(Final_Result)
        pool.close()
        pool.join()

    Final_Result_List = pd.concat(Final_Result_List)
    Final_Result_List.index = range(len(Final_Result_List))
    file_path = path_global.path_middle() + "//Features"
    if not os.path.exists(file_path + '//semi_std_'+direction):
        os.makedirs(file_path + '//semi_std_'+direction)
    os.chdir(file_path + '//semi_std_'+direction)
    Final_Result_List.to_csv(symbol + "_" + str(period) + ".csv")

###################################################################################################
from Multi_Processing import Spot_Snapshot_Single_bipower_var
def bipower_var(symbol, period, lag, begin_date=path_global.begin_date(), end_date= path_global.end_date()):
    ###############################################################################
    ### This function is for calculating the bipower-variance of middle price;
    ### INPUT : 1) symbol, e.g: "BTC"
    ###         2) period, period of return calculation, in seconds
    ###         3) lag, int, lagged period
    ###         4) begin_date, default in "2022-10-01"
    ###         5) end, default in "2022-10-01"
    ###############################################################################
    Path = path_global.path_spot() + "//binance//book_snapshot_25"
    os.chdir(Path + "//" + symbol)
    files = sorted(os.listdir())
    match = re.search(r"\d{4}-\d{2}-\d{2}", files[0])
    before, after = files[0][:match.start()], files[0][match.end():]
    Date_Range = DC.get_date_range(begin_date, end_date)

    cpu_num = 32
    Final_Result_List = []
    for i in range(len(Date_Range) // cpu_num + (len(Date_Range) % cpu_num > 0)):
        Final_Result = pd.DataFrame()
        date_range = Date_Range[i * cpu_num: (1 + i) * cpu_num]
        pool = mp.Pool(processes=cpu_num)
        results = [
            pool.apply_async(Spot_Snapshot_Single_bipower_var.process_data, args=(date, symbol, period, lag)) \
            for date in date_range]
        for result in tqdm(results):
            Final_Result = pd.concat([Final_Result, result.get()])
        Final_Result_List.append(Final_Result)
        pool.close()
        pool.join()

    Final_Result_List = pd.concat(Final_Result_List)
    Final_Result_List.index = range(len(Final_Result_List))
    file_path = path_global.path_middle() + "//Features"
    if not os.path.exists(file_path + '//bipower_var_lag_'+str(lag)):
        os.makedirs(file_path + '//bipower_var_lag_'+str(lag))
    os.chdir(file_path + '//bipower_var_lag_'+str(lag))
    Final_Result_List.to_csv(symbol + "_" + str(period) + ".csv")

###################################################################################################

