import os
import Global_Variables as GV
import Data_Clean as DC
import re
import pandas as pd
import multiprocessing as mp
from tqdm import tqdm

from Multi_Processing import Technique_Single_diff_middle_ma
def diff_middle_ma(symbol, long_period, short_period, begin_date=GV.begin_date(), end_date= GV.end_date()):
    ###############################################################################
    ### This function is for calculating difference of ma for midlle_price of given currency;
    ### INPUT : 1) symbol, e.g: "BTC"
    ###         2) short_period, period of short ma calculation
    ###         3) long_period, period of long ma calculation
    ###         4) begin_date, default in "2022-10-01"
    ###         5) end, default in "2022-10-01"
    ###############################################################################
    Path = GV.path_spot() + "//binance//book_snapshot_25"
    os.chdir(Path + "//"+symbol)
    files = sorted(os.listdir())
    match = re.search(r"\d{4}-\d{2}-\d{2}", files[0])
    Date_Range = DC.get_date_range(begin_date, end_date)

    cpu_num = 32
    Final_Result_List = []
    for i in range(len(Date_Range) // cpu_num + (len(Date_Range) % cpu_num > 0)):
        Final_Result = pd.DataFrame()
        date_range = Date_Range[i * cpu_num: (1 + i) * cpu_num]
        pool = mp.Pool(processes=cpu_num)
        results = [pool.apply_async(Technique_Single_diff_middle_ma.process_data, args=(date, symbol, long_period, short_period)) \
                   for date in date_range]
        for result in tqdm(results):
            Final_Result = pd.concat([Final_Result, result.get()])
        Final_Result_List.append(Final_Result)
        pool.close()
        pool.join()

    Final_Result_List = pd.concat(Final_Result_List)
    Final_Result_List.index = range(len(Final_Result_List))
    file_path = GV.path_middle() + "//Features"
    if not os.path.exists(file_path + '//diff_middle_ma'):
        os.makedirs(file_path + '//diff_middle_ma')
    os.chdir(file_path + '//diff_middle_ma')
    Final_Result_List.to_csv(f"{symbol}_{long_period}_{short_period}.csv")

################################################################################################
from Multi_Processing import Technique_Single_diff_middle_ema
def diff_middle_ema(symbol, long_period, short_period, begin_date=GV.begin_date(), end_date= GV.end_date()):
    ###############################################################################
    ### This function is for calculating difference of ema for midlle_price of given currency;
    ### INPUT : 1) symbol, e.g: "BTC"
    ###         2) short_period, period of short ma calculation
    ###         3) long_period, period of long ma calculation
    ###         4) begin_date, default in "2022-10-01"
    ###         5) end, default in "2022-10-01"
    ###############################################################################
    Path = GV.path_spot() + "//binance//book_snapshot_25"
    os.chdir(Path + "//" + symbol)
    files = sorted(os.listdir())
    match = re.search(r"\d{4}-\d{2}-\d{2}", files[0])
    Date_Range = DC.get_date_range(begin_date, end_date)

    cpu_num = 32
    Final_Result_List = []
    for i in range(len(Date_Range) // cpu_num + (len(Date_Range) % cpu_num > 0)):
        Final_Result = pd.DataFrame()
        date_range = Date_Range[i * cpu_num: (1 + i) * cpu_num]
        pool = mp.Pool(processes=cpu_num)
        results = [pool.apply_async(Technique_Single_diff_middle_ema.process_data,
                                    args=(date, symbol, long_period, short_period)) \
                   for date in date_range]
        for result in tqdm(results):
            Final_Result = pd.concat([Final_Result, result.get()])
        Final_Result_List.append(Final_Result)
        pool.close()
        pool.join()

    Final_Result_List = pd.concat(Final_Result_List)
    Final_Result_List.index = range(len(Final_Result_List))
    file_path = GV.path_middle() + "//Features"
    if not os.path.exists(file_path + '//diff_middle_ema'):
        os.makedirs(file_path + '//diff_middle_ema')
    os.chdir(file_path + '//diff_middle_ema')
    Final_Result_List.to_csv(f"{symbol}_{long_period}_{short_period}.csv")

################################################################################################
from Multi_Processing import Technique_Single_slow_sto
def slow_sto(symbol, period, begin_date=GV.begin_date(), end_date= GV.end_date()):
    ###############################################################################
    ### This function is for calculating slow stocastic for midlle_price of given currency;
    ### INPUT : 1) symbol, e.g: "BTC"
    ###         2) period, period of calculation
    ###         3) begin_date, default in "2022-10-01"
    ###         4) end, default in "2022-10-01"
    ###############################################################################
    Path = GV.path_spot() + "//binance//book_snapshot_25"
    os.chdir(Path + "//" + symbol)
    files = sorted(os.listdir())
    match = re.search(r"\d{4}-\d{2}-\d{2}", files[0])
    Date_Range = DC.get_date_range(begin_date, end_date)

    cpu_num = 32
    Final_Result_List = []
    for i in range(len(Date_Range) // cpu_num + (len(Date_Range) % cpu_num > 0)):
        Final_Result = pd.DataFrame()
        date_range = Date_Range[i * cpu_num: (1 + i) * cpu_num]
        pool = mp.Pool(processes=cpu_num)
        results = [pool.apply_async(Technique_Single_slow_sto.process_data,
                                    args=(date, symbol, period,)) \
                   for date in date_range]
        for result in tqdm(results):
            Final_Result = pd.concat([Final_Result, result.get()])
        Final_Result_List.append(Final_Result)
        pool.close()
        pool.join()

    Final_Result_List = pd.concat(Final_Result_List)
    Final_Result_List.index = range(len(Final_Result_List))
    file_path = GV.path_middle() + "//Features"
    if not os.path.exists(file_path + '//slow_sto'):
        os.makedirs(file_path + '//slow_sto')
    os.chdir(file_path + '//slow_sto')
    Final_Result_List.to_csv(f"{symbol}_{period}.csv")

################################################################################################
from Multi_Processing import Technique_Single_rsi
def rsi(symbol, period, begin_date=GV.begin_date(), end_date= GV.end_date()):
    ###############################################################################
    ### This function is for calculating rsi for midlle_price of given currency;
    ### INPUT : 1) symbol, e.g: "BTC"
    ###         2) period, period of calculation
    ###         3) begin_date, default in "2022-10-01"
    ###         4) end, default in "2022-10-01"
    ###############################################################################
    Path = GV.path_spot() + "//binance//book_snapshot_25"
    os.chdir(Path + "//" + symbol)
    files = sorted(os.listdir())
    match = re.search(r"\d{4}-\d{2}-\d{2}", files[0])
    Date_Range = DC.get_date_range(begin_date, end_date)

    cpu_num = 32
    Final_Result_List = []
    for i in range(len(Date_Range) // cpu_num + (len(Date_Range) % cpu_num > 0)):
        Final_Result = pd.DataFrame()
        date_range = Date_Range[i * cpu_num: (1 + i) * cpu_num]
        pool = mp.Pool(processes=cpu_num)
        results = [pool.apply_async(Technique_Single_rsi.process_data,
                                    args=(date, symbol, period,)) for date in date_range]
        for result in tqdm(results):
            Final_Result = pd.concat([Final_Result, result.get()])
        Final_Result_List.append(Final_Result)
        pool.close()
        pool.join()

    Final_Result_List = pd.concat(Final_Result_List)
    Final_Result_List.index = range(len(Final_Result_List))
    file_path = GV.path_middle() + "//Features"
    if not os.path.exists(file_path + '//rsi'):
        os.makedirs(file_path + '//rsi')
    os.chdir(file_path + '//rsi')
    Final_Result_List.to_csv(f"{symbol}_{period}.csv")

################################################################################################
from Multi_Processing import Technique_Single_disparity
def disparity(symbol, period, begin_date=GV.begin_date(), end_date= GV.end_date()):
    ###############################################################################
    ### This function is for calculating disparity for midlle_price of given currency;
    ### INPUT : 1) symbol, e.g: "BTC"
    ###         2) period, period of calculation
    ###         3) begin_date, default in "2022-10-01"
    ###         4) end, default in "2022-10-01"
    ###############################################################################
    Path = GV.path_spot() + "//binance//book_snapshot_25"
    os.chdir(Path + "//" + symbol)
    files = sorted(os.listdir())
    match = re.search(r"\d{4}-\d{2}-\d{2}", files[0])
    Date_Range = DC.get_date_range(begin_date, end_date)

    cpu_num = 32
    Final_Result_List = []
    for i in range(len(Date_Range) // cpu_num + (len(Date_Range) % cpu_num > 0)):
        Final_Result = pd.DataFrame()
        date_range = Date_Range[i * cpu_num: (1 + i) * cpu_num]
        pool = mp.Pool(processes=cpu_num)
        results = [pool.apply_async(Technique_Single_disparity.process_data,
                                    args=(date, symbol, period,)) for date in date_range]
        for result in tqdm(results):
            Final_Result = pd.concat([Final_Result, result.get()])
        Final_Result_List.append(Final_Result)
        pool.close()
        pool.join()

    Final_Result_List = pd.concat(Final_Result_List)
    Final_Result_List.index = range(len(Final_Result_List))
    file_path = GV.path_middle() + "//Features"
    if not os.path.exists(file_path + '//disparity'):
        os.makedirs(file_path + '//disparity')
    os.chdir(file_path + '//disparity')
    Final_Result_List.to_csv(f"{symbol}_{period}.csv")

