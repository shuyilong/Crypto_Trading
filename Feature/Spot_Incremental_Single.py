import os
import Global_Variables as GV
import Data_Clean as DC
import pandas as pd
import multiprocessing as mp
from tqdm import tqdm


from Multi_Processing import Spot_Incremental_Single_order_volumn
def order_volumn(symbol, period, data_type, direction, begin_date=GV.begin_date(), end_date= GV.end_date()):
###############################################################################
### INPUT : 1) symbol, e.g: "BTC"
###         2) period, period of return calculation, in seconds
###         3) direction, choose from "buy", "sell", "both"
###         4) begin_date, default in "2022-10-01"
###         5) end, default in "2022-10-01"
###############################################################################
    Path = GV.path_spot() + "//binance//incremental_book_L2"
    os.chdir(Path + "//" + symbol)
    Date_Range = DC.get_date_range(begin_date, end_date)

    cpu_num = 32
    Final_Result_List = []
    for i in range(len(Date_Range) // cpu_num + (len(Date_Range) % cpu_num > 0)):
        Final_Result = pd.DataFrame()
        date_range = Date_Range[i * cpu_num: (1 + i) * cpu_num]
        pool = mp.Pool(processes=cpu_num)
        results = [pool.apply_async(Spot_Incremental_Single_order_volumn.process_data, \
                        args=(date, symbol, period, data_type, direction)) for date in date_range]
        for result in tqdm(results):
            Final_Result = pd.concat([Final_Result, result.get()])
        Final_Result_List.append(Final_Result)
        pool.close()
        pool.join()

    Final_Result_List = pd.concat(Final_Result_List)
    Final_Result_List.index = range(len(Final_Result_List))
    file_path = GV.path_middle() + "//Features"
    if not os.path.exists(file_path + '//order_volumn'):
        os.makedirs(file_path + '//order_volumn')
    os.chdir(file_path + '//order_volumn')
    Final_Result_List.to_csv(f"{symbol}_{period}_{data_type}_{direction}_{begin_date}_{end_date}.csv")

#################################################################################################
from Multi_Processing import Spot_Incremental_Single_order_volumn_imbalance
def order_volumn_imbalance(symbol, period, begin_date=GV.begin_date(), end_date= GV.end_date()):
###############################################################################
### INPUT : 1) symbol, e.g: "BTC"
###         2) period, period of return calculation, in seconds
###         3) begin_date, default in "2022-10-01"
###         4) end, default in "2022-10-01"
###############################################################################
    Path = GV.path_spot() + "//binance//incremental_book_L2"
    os.chdir(Path + "//" + symbol)
    Date_Range = DC.get_date_range(begin_date, end_date)

    cpu_num = 32
    Final_Result_List = []
    for i in range(len(Date_Range) // cpu_num + (len(Date_Range) % cpu_num > 0)):
        Final_Result = pd.DataFrame()
        date_range = Date_Range[i * cpu_num: (1 + i) * cpu_num]
        pool = mp.Pool(processes=cpu_num)
        results = [pool.apply_async(Spot_Incremental_Single_order_volumn_imbalance.process_data,
                                    args=(date, symbol, period)) for date in date_range]
        for result in tqdm(results):
            Final_Result = pd.concat([Final_Result, result.get()])
        Final_Result_List.append(Final_Result)
        pool.close()
        pool.join()

    Final_Result_List = pd.concat(Final_Result_List)
    Final_Result_List.index = range(len(Final_Result_List))
    file_path = GV.path_middle() + "//Features"
    if not os.path.exists(file_path + '//order_volumn_imbalance'):
        os.makedirs(file_path + '//order_volumn_imbalance')
    os.chdir(file_path + '//order_volumn_imbalance')
    Final_Result_List.to_csv(f"{symbol}_{period}_{begin_date}_{end_date}.csv")

#################################################################################################
from Multi_Processing import Spot_Incremental_Single_order_frequency
def order_frequency(symbol, period, data_type, direction, begin_date=GV.begin_date(), end_date= GV.end_date()):
###############################################################################
### INPUT : 1) symbol, e.g: "BTC"
###         2) period, period of return calculation, in seconds
###         3) begin_date, default in "2022-10-01"
###         4) end, default in "2022-10-01"
###############################################################################
    Path = GV.path_spot() + "//binance//incremental_book_L2"
    os.chdir(Path + "//" + symbol)
    Date_Range = DC.get_date_range(begin_date, end_date)

    cpu_num = 32
    Final_Result_List = []
    for i in range(len(Date_Range) // cpu_num + (len(Date_Range) % cpu_num > 0)):
        Final_Result = pd.DataFrame()
        date_range = Date_Range[i * cpu_num: (1 + i) * cpu_num]
        pool = mp.Pool(processes=cpu_num)
        results = [pool.apply_async(Spot_Incremental_Single_order_frequency.process_data,
                                    args=(date, symbol, period, data_type, direction)) for date in date_range]
        for result in tqdm(results):
            Final_Result = pd.concat([Final_Result, result.get()])
        Final_Result_List.append(Final_Result)
        pool.close()
        pool.join()

    Final_Result_List = pd.concat(Final_Result_List)
    Final_Result_List.index = range(len(Final_Result_List))
    file_path = GV.path_middle() + "//Features"
    if not os.path.exists(file_path + '//order_frequency'):
        os.makedirs(file_path + '//order_frequency')
    os.chdir(file_path + '//order_frequency')
    Final_Result_List.to_csv(f"{symbol}_{period}_{data_type}_{direction}_{begin_date}_{end_date}.csv")

#################################################################################################
from Multi_Processing import Spot_Incremental_Single_order_frequency_imbalance
def order_frequency_imbalance(symbol, period, begin_date=GV.begin_date(), end_date= GV.end_date()):
###############################################################################
### INPUT : 1) symbol, e.g: "BTC"
###         2) period, period of return calculation, in seconds
###         3) begin_date, default in "2022-10-01"
###         4) end, default in "2022-10-01"
###############################################################################
    Path = GV.path_spot() + "//binance//incremental_book_L2"
    os.chdir(Path + "//" + symbol)
    Date_Range = DC.get_date_range(begin_date, end_date)

    cpu_num = 32
    Final_Result_List = []
    for i in range(len(Date_Range) // cpu_num + (len(Date_Range) % cpu_num > 0)):
        Final_Result = pd.DataFrame()
        date_range = Date_Range[i * cpu_num: (1 + i) * cpu_num]
        pool = mp.Pool(processes=cpu_num)
        results = [pool.apply_async(Spot_Incremental_Single_order_frequency_imbalance.process_data,
                                    args=(date, symbol, period)) for date in date_range]
        for result in tqdm(results):
            Final_Result = pd.concat([Final_Result, result.get()])
        Final_Result_List.append(Final_Result)
        pool.close()
        pool.join()

    Final_Result_List = pd.concat(Final_Result_List)
    Final_Result_List.index = range(len(Final_Result_List))
    file_path = GV.path_middle() + "//Features"
    if not os.path.exists(file_path + '//order_frequency_imbalance'):
        os.makedirs(file_path + '//order_frequency_imbalance')
    os.chdir(file_path + '//order_frequency_imbalance')
    Final_Result_List.to_csv(f"{symbol}_{period}_{begin_date}_{end_date}.csv")

#################################################################################################
from Multi_Processing import Spot_Incremental_Single_order_volumn_derivative
def order_volumn_derivative(symbol, period, direction, begin_date=GV.begin_date(), end_date= GV.end_date()):
###############################################################################
### INPUT : 1) symbol, e.g: "BTC"
###         2) period, period of return calculation, in seconds
###         3) direction, choose from "buy", "sell", "both"
###         4) begin_date, default in "2022-10-01"
###         5) end, default in "2022-10-01"
###############################################################################
    Path = GV.path_spot() + "//binance//incremental_book_L2"
    os.chdir(Path + "//" + symbol)
    Date_Range = DC.get_date_range(begin_date, end_date)

    cpu_num = 32
    Final_Result_List = []
    for i in range(len(Date_Range) // cpu_num + (len(Date_Range) % cpu_num > 0)):
        Final_Result = pd.DataFrame()
        date_range = Date_Range[i * cpu_num: (1 + i) * cpu_num]
        pool = mp.Pool(processes=cpu_num)
        results = [pool.apply_async(Spot_Incremental_Single_order_volumn_derivative.process_data,
                                    args=(date, symbol, period, direction)) for date in date_range]
        for result in tqdm(results):
            Final_Result = pd.concat([Final_Result, result.get()])
        Final_Result_List.append(Final_Result)
        pool.close()
        pool.join()

    Final_Result_List = pd.concat(Final_Result_List)
    Final_Result_List.index = range(len(Final_Result_List))
    file_path = GV.path_middle() + "//Features"
    if not os.path.exists(file_path + '//order_volumn_derivative'):
        os.makedirs(file_path + '//order_volumn_derivative')
    os.chdir(file_path + '//order_volumn_derivative')
    Final_Result_List.to_csv(f"{symbol}_{period}_{direction}_{begin_date}_{end_date}.csv")

#################################################################################################
from Multi_Processing import Spot_Incremental_Single_order_frequency_derivative
def order_frequency_derivative(symbol, period, direction, begin_date=GV.begin_date(), end_date= GV.end_date()):
###############################################################################
### INPUT : 1) symbol, e.g: "BTC"
###         2) period, period of return calculation, in seconds
###         3) direction, choose from "buy", "sell", "both"
###         4) begin_date, default in "2022-10-01"
###         5) end, default in "2022-10-01"
###############################################################################
    Path = GV.path_spot() + "//binance//incremental_book_L2"
    os.chdir(Path + "//" + symbol)
    Date_Range = DC.get_date_range(begin_date, end_date)

    cpu_num = 32
    Final_Result_List = []
    for i in range(len(Date_Range) // cpu_num + (len(Date_Range) % cpu_num > 0)):
        Final_Result = pd.DataFrame()
        date_range = Date_Range[i * cpu_num: (1 + i) * cpu_num]
        pool = mp.Pool(processes=cpu_num)
        results = [pool.apply_async(Spot_Incremental_Single_order_frequency_derivative.process_data,
                                    args=(date, symbol, period, direction)) for date in date_range]
        for result in tqdm(results):
            Final_Result = pd.concat([Final_Result, result.get()])
        Final_Result_List.append(Final_Result)
        pool.close()
        pool.join()

    Final_Result_List = pd.concat(Final_Result_List)
    Final_Result_List.index = range(len(Final_Result_List))
    file_path = GV.path_middle() + "//Features"
    if not os.path.exists(file_path + '//order_frequency_derivative'):
        os.makedirs(file_path + '//order_frequency_derivative')
    os.chdir(file_path + '//order_frequency_derivative')
    Final_Result_List.to_csv(f"{symbol}_{period}_{direction}_{begin_date}_{end_date}.csv")

#################################################################################################
from Multi_Processing import Spot_Incremental_Single_order_volumn_derivative_2nd
def order_volumn_derivative_2nd(symbol, period, direction, begin_date=GV.begin_date(), end_date= GV.end_date()):
###############################################################################
### INPUT : 1) symbol, e.g: "BTC"
###         2) period, period of return calculation, in seconds
###         3) direction, choose from "buy", "sell", "both"
###         4) begin_date, default in "2022-10-01"
###         5) end, default in "2022-10-01"
###############################################################################
    Path = GV.path_spot() + "//binance//incremental_book_L2"
    os.chdir(Path + "//" + symbol)
    Date_Range = DC.get_date_range(begin_date, end_date)

    cpu_num = 32
    Final_Result_List = []
    for i in range(len(Date_Range) // cpu_num + (len(Date_Range) % cpu_num > 0)):
        Final_Result = pd.DataFrame()
        date_range = Date_Range[i * cpu_num: (1 + i) * cpu_num]
        pool = mp.Pool(processes=cpu_num)
        results = [pool.apply_async(Spot_Incremental_Single_order_volumn_derivative_2nd.process_data,
                                    args=(date, symbol, period, direction)) for date in date_range]
        for result in tqdm(results):
            Final_Result = pd.concat([Final_Result, result.get()])
        Final_Result_List.append(Final_Result)
        pool.close()
        pool.join()

    Final_Result_List = pd.concat(Final_Result_List)
    Final_Result_List.index = range(len(Final_Result_List))
    file_path = GV.path_middle() + "//Features"
    if not os.path.exists(file_path + '//order_volumn_derivative_2nd'):
        os.makedirs(file_path + '//order_volumn_derivative_2nd')
    os.chdir(file_path + '//order_volumn_derivative_2nd')
    Final_Result_List.to_csv(f"{symbol}_{period}_{direction}_{begin_date}_{end_date}.csv")

#################################################################################################
from Multi_Processing import Spot_Incremental_Single_order_frequency_derivative_2nd
def order_frequency_derivative_2nd(symbol, period, direction, begin_date=GV.begin_date(), end_date= GV.end_date()):
###############################################################################
### INPUT : 1) symbol, e.g: "BTC"
###         2) period, period of return calculation, in seconds
###         3) direction, choose from "buy", "sell", "both"
###         4) begin_date, default in "2022-10-01"
###         5) end, default in "2022-10-01"
###############################################################################
    Path = GV.path_spot() + "//binance//incremental_book_L2"
    os.chdir(Path + "//" + symbol)
    Date_Range = DC.get_date_range(begin_date, end_date)

    cpu_num = 32
    Final_Result_List = []
    for i in range(len(Date_Range) // cpu_num + (len(Date_Range) % cpu_num > 0)):
        Final_Result = pd.DataFrame()
        date_range = Date_Range[i * cpu_num: (1 + i) * cpu_num]
        pool = mp.Pool(processes=cpu_num)
        results = [pool.apply_async(Spot_Incremental_Single_order_frequency_derivative_2nd.process_data,
                                    args=(date, symbol, period, direction)) for date in date_range]
        for result in tqdm(results):
            Final_Result = pd.concat([Final_Result, result.get()])
        Final_Result_List.append(Final_Result)
        pool.close()
        pool.join()

    Final_Result_List = pd.concat(Final_Result_List)
    Final_Result_List.index = range(len(Final_Result_List))
    file_path = GV.path_middle() + "//Features"
    if not os.path.exists(file_path + '//order_frequency_derivative_2nd'):
        os.makedirs(file_path + '//order_frequency_derivative_2nd')
    os.chdir(file_path + '//order_frequency_derivative_2nd')
    Final_Result_List.to_csv(f"{symbol}_{period}_{direction}_{begin_date}_{end_date}.csv")
