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
'''
def diff_ma(symbol, long_period, short_period, begin_date=path_global.begin_date(), end_date= path_global.end_date()):
    ###############################################################################
    ### This function is for calculating difference of ma for midlle_price of given currency;
    ### INPUT : 1) symbol, e.g: "BTC"
    ###         2) short_period, period of short ma calculation
    ###         3) long_period, period of long ma calculation
    ###         4) begin_date, default in "2022-10-01"
    ###         5) end, default in "2022-10-01"
    ###############################################################################
    Path = path_global.path_spot() + "//binance//book_snapshot_25"
    os.chdir(Path + "//"+symbol)
    files = sorted(os.listdir())
    match = re.search(r"\d{4}-\d{2}-\d{2}", files[0])
    before, after = files[0][:match.start()], files[0][match.end():]
    Date_Range = DC.get_date_range(begin_date, end_date)
'''