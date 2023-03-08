import os
import Global_Variables as GV
import Data_Clean as DC
import pandas as pd
import multiprocessing as mp
from tqdm import tqdm

from Multi_Processing import Spot_Snapshot_Single_best_diff
def best_diff(symbol, period, direction, begin_date=GV.begin_date(), end_date= GV.end_date()):
    ###############################################################################
    ### INPUT : 1) symbol, e.g: "BTC"
    ###         2) period, period of return calculation, in seconds
    ###         3) direction, choose from "ask" and "bid"
    ###         3) begin_date, default in "2022-10-01"
    ###         4) end, default in "2023-02-21"
    ###############################################################################
    Path = GV.path_spot() + "//binance//book_snapshot_25"
    os.chdir(Path + "//"+symbol)
    Date_Range = DC.get_date_range(begin_date, end_date)

    cpu_num = 32
    Final_Result_List = []
    for i in range(len(Date_Range) // cpu_num + (len(Date_Range) % cpu_num > 0)):
        Final_Result = pd.DataFrame()
        date_range = Date_Range[i*cpu_num : (1+i) * cpu_num]
        pool = mp.Pool(processes= cpu_num)
        results = [pool.apply_async(Spot_Snapshot_Single_best_diff.process_data, \
                        args=(date, symbol, period, direction)) for date in date_range]
        for result in tqdm(results):
            Final_Result = pd.concat([Final_Result, result.get()])
        Final_Result_List.append(Final_Result)
        pool.close()
        pool.join()

    Final_Result_List = pd.concat(Final_Result_List)
    Final_Result_List.index = range(len(Final_Result_List))
    file_path = GV.path_middle() + "//Features"
    if not os.path.exists(file_path + '//best_diff'):
        os.makedirs(file_path + '//best_diff')
    os.chdir(file_path + '//best_diff')
    Final_Result_List.to_csv(f"{symbol}_{period}_{direction}_{begin_date}_{end_date}.csv")

###################################################################################################
from Multi_Processing import Spot_Snapshot_Single_n_depth
def n_depth(symbol, period, n, data_type, direction, begin_date=GV.begin_date(), end_date= GV.end_date()):
    ###############################################################################
    ### INPUT : 1) symbol, e.g: "BTC"
    ###         2) period, period of return calculation, in seconds
    ###         3) n, You want to study the price of the nth block.
    ###         4) data_type, choose from "mean","max","min","std","median","sum"
    ###         5) direction, choose from "ask" and "bid"
    ###         5) begin_date, default in "2022-10-01"
    ###         6) end, default in "2023-02-21"
    ###############################################################################
    Path = GV.path_spot() + "//binance//book_snapshot_25"
    os.chdir(Path + "//" + symbol)
    Date_Range = DC.get_date_range(begin_date, end_date)

    cpu_num = 32
    Final_Result_List = []
    for i in range(len(Date_Range) // cpu_num + (len(Date_Range) % cpu_num > 0)):
        Final_Result = pd.DataFrame()
        date_range = Date_Range[i*cpu_num : (1+i) * cpu_num]
        pool = mp.Pool(processes= cpu_num)
        results = [pool.apply_async(Spot_Snapshot_Single_n_depth.process_data, \
                     args=(date, symbol, period, n, data_type, direction)) for date in date_range]
        for result in tqdm(results):
            Final_Result = pd.concat([Final_Result, result.get()])
        Final_Result_List.append(Final_Result)
        pool.close()
        pool.join()

    Final_Result_List = pd.concat(Final_Result_List)
    Final_Result_List.index = range(len(Final_Result_List))
    file_path = GV.path_middle() + "//Features"
    if not os.path.exists(file_path + '//n_depth'):
        os.makedirs(file_path + '//n_depth')
    os.chdir(file_path + '//n_depth')
    Final_Result_List.to_csv(f"{symbol}_{period}_{n}_{data_type}_{direction}_{begin_date}_{end_date}.csv")

###################################################################################################
def middle_mom(symbol, period, begin_date=GV.begin_date(), end_date= GV.end_date()):
    ###############################################################################
    ### INPUT : 1) symbol, e.g: "BTC"
    ###         2) period, period of return calculation, in seconds
    ###         4) begin_date, default in "2022-10-01"
    ###         5) end, default in "2023-02-21"
    ###############################################################################
    Path = GV.path_middle_second_data()
    os.chdir(Path)
    file = pd.read_csv(symbol + "_Second_data.csv")
    file['second_timestamp'] = pd.to_datetime(file['second_timestamp'], unit='s')
    file = file.set_index('second_timestamp')
    all_seconds = pd.date_range(start=file.index.min(), end=file.index.max(), freq='1s')
    file = file.reindex(all_seconds).fillna(method="ffill")
    file['feature'] = ((file['middle_price'] / file['middle_price'].shift(period) - 1) * 10000).shift(1)
    file['second_timestamp'] = pd.DatetimeIndex(file.index).astype(int) // 10 ** 9

    start_time = pd.Timestamp(begin_date + ' 00:00:00')
    end_time = pd.Timestamp(end_date + ' 23:59:59')
    mask = file.index.slice_indexer(start_time, end_time)
    file = file[['second_timestamp', 'feature']].iloc[mask]
    file.index = range(len(file))
    file_path = GV.path_middle() + "//Features"
    if not os.path.exists(file_path + '//window_return'):
        os.makedirs(file_path + '//window_return')
    os.chdir(file_path + '//window_return')
    file.to_csv(f"{symbol}_{period}.csv")

###################################################################################################
from Multi_Processing import Spot_Snapshot_Single_spread_return
def spread_return(symbol, period, n, data_type, begin_date=GV.begin_date(), end_date= GV.end_date()):
    ###############################################################################
    ### INPUT : 1) symbol, e.g: "BTC"
    ###         2) period, period of return calculation, in seconds
    ###         3) data_type, choose from "mean","max","min","std","median","sum"
    ###         4) n, n-th ask and bid, from 0 to 24
    ###         5) begin_date, default in "2022-10-01"
    ###         6) end, default in "2023-02-21"
    ###############################################################################
    Path = GV.path_spot() + "//binance//book_snapshot_25"
    os.chdir(Path + "//" + symbol)
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
    file_path = GV.path_middle() + "//Features"
    if not os.path.exists(file_path + '//spread_return'):
        os.makedirs(file_path + '//spread_return')
    os.chdir(file_path + '//spread_return')
    Final_Result_List.to_csv(f"{symbol}_{period}_{n}_{data_type}_{begin_date}_{end_date}.csv")

###################################################################################################
from Multi_Processing import Spot_Snapshot_Single_middle_derivative
def middle_derivative(symbol, period, begin_date=GV.begin_date(), end_date= GV.end_date()):
    ###############################################################################
    ### INPUT : 1) symbol, e.g: "BTC"
    ###         2) period, period of return calculation, in seconds
    ###         3) data_type, choose from "mean","max","min","std","median","sum"
    ###         4) n, n-th ask and bid, from 0 to 24
    ###         5) begin_date, default in "2022-10-01"
    ###         6) end, default in "2022-10-01"
    ###############################################################################
    Path = GV.path_spot() + "//binance//book_snapshot_25"
    os.chdir(Path + "//" + symbol)
    Date_Range = DC.get_date_range(begin_date, end_date)

    cpu_num = 32
    Final_Result_List = []
    for i in range(len(Date_Range) // cpu_num + (len(Date_Range) % cpu_num > 0)):
        Final_Result = pd.DataFrame()
        date_range = Date_Range[i * cpu_num: (1 + i) * cpu_num]
        pool = mp.Pool(processes=cpu_num)
        results = [
            pool.apply_async(Spot_Snapshot_Single_middle_derivative.process_data, \
                    args=(date, symbol, period)) for date in date_range]
        for result in tqdm(results):
            Final_Result = pd.concat([Final_Result, result.get()])
        Final_Result_List.append(Final_Result)
        pool.close()
        pool.join()

    Final_Result_List = pd.concat(Final_Result_List)
    Final_Result_List.index = range(len(Final_Result_List))
    file_path = GV.path_middle() + "//Features"
    if not os.path.exists(file_path + '//middle_derivative'):
        os.makedirs(file_path + '//middle_derivative')
    os.chdir(file_path + '//middle_derivative')
    Final_Result_List.to_csv(f"{symbol}_{period}_{begin_date}_{end_date}.csv")

###################################################################################################
from Multi_Processing import Spot_Snapshot_Single_middle_derivative_2nd
def middle_derivative_2nd(symbol, period, begin_date=GV.begin_date(), end_date= GV.end_date()):
    ###############################################################################
    ### INPUT : 1) symbol, e.g: "BTC"
    ###         2) period, period of return calculation, in seconds
    ###         3) data_type, choose from "mean","max","min","std","median","sum"
    ###         4) n, n-th ask and bid, from 0 to 24
    ###         5) begin_date, default in "2022-10-01"
    ###         6) end, default in "2022-10-01"
    ###############################################################################
    Path = GV.path_spot() + "//binance//book_snapshot_25"
    os.chdir(Path + "//" + symbol)
    Date_Range = DC.get_date_range(begin_date, end_date)

    cpu_num = 32
    Final_Result_List = []
    for i in range(len(Date_Range) // cpu_num + (len(Date_Range) % cpu_num > 0)):
        Final_Result = pd.DataFrame()
        date_range = Date_Range[i * cpu_num: (1 + i) * cpu_num]
        pool = mp.Pool(processes=cpu_num)
        results = [
            pool.apply_async(Spot_Snapshot_Single_middle_derivative_2nd.process_data, \
                    args=(date, symbol, period)) for date in date_range]
        for result in tqdm(results):
            Final_Result = pd.concat([Final_Result, result.get()])
        Final_Result_List.append(Final_Result)
        pool.close()
        pool.join()

    Final_Result_List = pd.concat(Final_Result_List)
    Final_Result_List.index = range(len(Final_Result_List))
    file_path = GV.path_middle() + "//Features"
    if not os.path.exists(file_path + '//middle_derivative_2nd'):
        os.makedirs(file_path + '//middle_derivative_2nd')
    os.chdir(file_path + '//middle_derivative_2nd')
    Final_Result_List.to_csv(f"{symbol}_{period}_{begin_date}_{end_date}.csv")

###################################################################################################
from Multi_Processing import Spot_Snapshot_Single_middle_std
def middle_std(symbol, period, direction, begin_date=GV.begin_date(), end_date= GV.end_date()):
    ###############################################################################
    ### INPUT : 1) symbol, e.g: "BTC"
    ###         2) period, period of return calculation, in seconds
    ###         3) direction, choose from "positive","negative","total"
    ###         4) begin_date, default in "2022-10-01"
    ###         5) end, default in "2022-10-01"
    ###############################################################################
    Path = GV.path_spot() + "//binance//book_snapshot_25"
    os.chdir(Path + "//" + symbol)
    Date_Range = DC.get_date_range(begin_date, end_date)

    cpu_num = 32
    Final_Result_List = []
    for i in range(len(Date_Range) // cpu_num + (len(Date_Range) % cpu_num > 0)):
        Final_Result = pd.DataFrame()
        date_range = Date_Range[i * cpu_num: (1 + i) * cpu_num]
        pool = mp.Pool(processes=cpu_num)
        results = [
            pool.apply_async(Spot_Snapshot_Single_middle_std.process_data, \
                   args=(date, symbol, period, direction)) for date in date_range]
        for result in tqdm(results):
            Final_Result = pd.concat([Final_Result, result.get()])
        Final_Result_List.append(Final_Result)
        pool.close()
        pool.join()

    Final_Result_List = pd.concat(Final_Result_List)
    Final_Result_List.index = range(len(Final_Result_List))
    file_path = GV.path_middle() + "//Features"
    if not os.path.exists(file_path + '//middle_std'):
        os.makedirs(file_path + '//middle_std')
    os.chdir(file_path + '//middle_std')
    Final_Result_List.to_csv(f"{symbol}_{period}_{direction}_{begin_date}_{end_date}.csv")

###################################################################################################
from Multi_Processing import Spot_Snapshot_Single_bipower_var
def bipower_var(symbol, period, lag, begin_date=GV.begin_date(), end_date= GV.end_date()):
    ###############################################################################
    ### INPUT : 1) symbol, e.g: "BTC"
    ###         2) period, period of return calculation, in seconds
    ###         3) lag, int, lagged period
    ###         4) begin_date, default in "2022-10-01"
    ###         5) end, default in "2022-10-01"
    ###############################################################################
    Path = GV.path_spot() + "//binance//book_snapshot_25"
    os.chdir(Path + "//" + symbol)
    Date_Range = DC.get_date_range(begin_date, end_date)

    cpu_num = 32
    Final_Result_List = []
    for i in range(len(Date_Range) // cpu_num + (len(Date_Range) % cpu_num > 0)):
        Final_Result = pd.DataFrame()
        date_range = Date_Range[i * cpu_num: (1 + i) * cpu_num]
        pool = mp.Pool(processes=cpu_num)
        results = [
            pool.apply_async(Spot_Snapshot_Single_bipower_var.process_data, \
                             args=(date, symbol, period, lag)) for date in date_range]
        for result in tqdm(results):
            Final_Result = pd.concat([Final_Result, result.get()])
        Final_Result_List.append(Final_Result)
        pool.close()
        pool.join()

    Final_Result_List = pd.concat(Final_Result_List)
    Final_Result_List.index = range(len(Final_Result_List))
    file_path = GV.path_middle() + "//Features"
    if not os.path.exists(file_path + '//bipower_var'):
        os.makedirs(file_path + '//bipower_var')
    os.chdir(file_path + '//bipower_var')
    Final_Result_List.to_csv(f"{symbol}_{period}_{lag}_{begin_date}_{end_date}.csv")

###################################################################################################
from Multi_Processing import Spot_Snapshot_Single_realized_quarticity
def realized_quarticity(symbol, period, begin_date=GV.begin_date(), end_date= GV.end_date()):
    ###############################################################################
    ### INPUT : 1) symbol, e.g: "BTC"
    ###         2) period, period of return calculation, in seconds
    ###         3) begin_date, default in "2022-10-01"
    ###         4) end, default in "2022-10-01"
    ###############################################################################
    Path = GV.path_spot() + "//binance//book_snapshot_25"
    os.chdir(Path + "//" + symbol)
    Date_Range = DC.get_date_range(begin_date, end_date)

    cpu_num = 32
    Final_Result_List = []
    for i in range(len(Date_Range) // cpu_num + (len(Date_Range) % cpu_num > 0)):
        Final_Result = pd.DataFrame()
        date_range = Date_Range[i * cpu_num: (1 + i) * cpu_num]
        pool = mp.Pool(processes=cpu_num)
        results = [
            pool.apply_async(Spot_Snapshot_Single_realized_quarticity.process_data, \
                             args=(date, symbol, period)) for date in date_range]
        for result in tqdm(results):
            Final_Result = pd.concat([Final_Result, result.get()])
        Final_Result_List.append(Final_Result)
        pool.close()
        pool.join()

    Final_Result_List = pd.concat(Final_Result_List)
    Final_Result_List.index = range(len(Final_Result_List))
    file_path = GV.path_middle() + "//Features"
    if not os.path.exists(file_path + '//realized_quarticity'):
        os.makedirs(file_path + '//realized_quarticity')
    os.chdir(file_path + '//realized_quarticity')
    Final_Result_List.to_csv(f"{symbol}_{period}_{begin_date}_{end_date}.csv")

###################################################################################################
from Multi_Processing import Spot_Snapshot_Single_snapshot_vol_imbalance
def snapshot_vol_imbalance(symbol, period, n, data_type, begin_date=GV.begin_date(), end_date= GV.end_date()):
    ###############################################################################
    ### INPUT : 1) symbol, e.g: "BTC"
    ###         2) period, period of return calculation, in seconds
    ###         3) n, n-th ask and bid, from 0 to 24
    ###         4) begin_date, default in "2022-10-01"
    ###         5) end, default in "2022-10-01"
    ###############################################################################
    Path = GV.path_spot() + "//binance//book_snapshot_25"
    os.chdir(Path + "//" + symbol)
    Date_Range = DC.get_date_range(begin_date, end_date)

    cpu_num = 32
    Final_Result_List = []
    for i in range(len(Date_Range) // cpu_num + (len(Date_Range) % cpu_num > 0)):
        Final_Result = pd.DataFrame()
        date_range = Date_Range[i * cpu_num: (1 + i) * cpu_num]
        pool = mp.Pool(processes=cpu_num)
        results = [
            pool.apply_async(Spot_Snapshot_Single_snapshot_vol_imbalance.process_data, \
                             args=(date, symbol, period, n, data_type)) for date in date_range]
        for result in tqdm(results):
            Final_Result = pd.concat([Final_Result, result.get()])
        Final_Result_List.append(Final_Result)
        pool.close()
        pool.join()

    Final_Result_List = pd.concat(Final_Result_List)
    Final_Result_List.index = range(len(Final_Result_List))
    file_path = GV.path_middle() + "//Features"
    if not os.path.exists(file_path + '//snapshot_vol_imbalance'):
        os.makedirs(file_path + '//snapshot_vol_imbalance')
    os.chdir(file_path + '//snapshot_vol_imbalance')
    Final_Result_List.to_csv(f"{symbol}_{period}_{n}_{data_type}_{begin_date}_{end_date}.csv")
