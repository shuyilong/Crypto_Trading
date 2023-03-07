import os
import pandas as pd
import Data_Clean as DC
import Global_Variables as GV

def future_return_diff(pair, period, start=GV.begin_date(), end = GV.end_date()):
    ###############################################################################
    ### This function is for calculating ret difference between 2 currency;
    ### And you should choose the calculation period of return as freq;
    ### The return here refers to the return of the future period calculated today;
    ### INPUT : 1) pair, 2 currencies you want to display, e.g: ["BTC","ETH"]
    ###         2) period, period of return calculation, in seconds
    ###         3) start, Start time of statistics
    ###         4) end, End time of statistics
    ###############################################################################
    os.chdir(GV.path_middle() + "//" + "Middle_Second_Data")
    data_1 = pd.read_csv(pair[0] +"_Second_Data.csv")[['second_timestamp', 'middle_price']]
    data_2 = pd.read_csv(pair[1] +"_Second_Data.csv")[['second_timestamp', 'middle_price']]
    data_1['second_timestamp'] = pd.to_datetime(data_1['second_timestamp'], unit='s')
    data_1 = data_1.set_index('second_timestamp')
    data_2['second_timestamp'] = pd.to_datetime(data_2['second_timestamp'], unit='s')
    data_2 = data_2.set_index('second_timestamp')

    Close_Price = pd.merge(data_1, data_2, how='left', on='second_timestamp', suffixes=('_'+pair[0], '_'+pair[1]))
    ### Calculate return according to the specified period
    Close_Price['ret_' + pair[0]] = (Close_Price['middle_price_' + pair[0]].shift(-period + 1) / Close_Price['middle_price_' + pair[0]].shift(1)) -1
    Close_Price['ret_' + pair[1]] = (Close_Price['middle_price_' + pair[1]].shift(-period + 1) / Close_Price['middle_price_' + pair[1]].shift(1)) -1
    Close_Price['diff'] = Close_Price['ret_' + pair[0]] - Close_Price['ret_' + pair[1]]
    Close_Price['second_timestamp'] = Close_Price.index
    Close_Price = Close_Price.loc[start:end]
    os.chdir(GV.path_middle() + "//" + "Future_Return_Diff")
    Close_Price[['second_timestamp','diff']].to_csv(f"{pair[0]}_{pair[1]}_{period}_{start}_{end}.csv")