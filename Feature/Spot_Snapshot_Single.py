import os
from Global_Variables import path_global
import Data_Clean as DC
import re
import pandas as pd
import time

def best_bid_diff(symbol, period, begin_date="2022-10-01", end_date= "2023-02-21"):
    os.chdir(path_global.path_middle() + '//Spot_Snapshot_Second_Data')

            ### Get Statistic Data
            Second_Data = data[['time','asks[0].price']].groupby('time').agg({'asks[0].price': ['mean'], \
                                'bids[0].price': ['mean'], 'spread':['mean'], 'ask_depth_1':['mean'],\
                                'ask_depth_5':['mean'], 'ask_depth_15':['mean'], 'bid_depth_1':['mean'],\
                                'bid_depth_5':['mean'], 'bid_depth_15':['mean']})