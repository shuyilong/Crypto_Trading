import os
import pandas as pd
import Data_Clean as DC
from Global_Variables import path

def future_return_diff(pair, period, start="2022-10-02 00:00:00", end = "2023-02-21 00:00:00"):
    ###############################################################################
    ### This function is for calculating ret difference between 2 currency;
    ### And you should choose the calculation period of return as freq;
    ### The return here refers to the return of the future period calculated today;
    ### INPUT : 1) pair, 2 currencies you want to display, e.g: ["BTC","ETH"]
    ###         2) period, period of return calculation, in seconds
    ###         3) start, Start time of statistics
    ###         4) end, End time of statistics
    ###############################################################################
    os.chdir(path.path_middle + "//" + "Trade_Second_Data")
    data_1 = pd.read_csv(pair[0 ] +"_Second_Data.csv")[['time', 'price']]
    data_2 = pd.read_csv(pair[1 ] +"_Second_Data.csv")[['time', 'price']]

    time_range = DC.time_interval("2022-10-01 00:00:00", \
                                  "2023-02-20 23:59:59", 1)
    Close_Price = pd.DataFrame(time_range ,columns = ['time'])
    Close_Price = pd.merge(Close_Price, data_1[['time' ,'price']], how='left', on='time')
    Close_Price = pd.merge(Close_Price, data_2[['time' ,'price']], how='left', on='time')
    column_name = ['time']
    column_name.extend(pair)
    Close_Price.columns = column_name
    Close_Price = Close_Price.fillna(method='ffill')
    Close_Price = Close_Price.fillna(method='bfill')
    ### Calculate return according to the specified frequency
    Close_Price[pair[0]] = Close_Price[pair[0]].shift(-period) / Close_Price[pair[0]]
    Close_Price[pair[1]] = Close_Price[pair[1]].shift(-period) / Close_Price[pair[1]]
    Close_Price['diff'] = Close_Price[pair[0]] - Close_Price[pair[1]]
    Close_Price = Close_Price[(Close_Price['time'] >= start) & \
                              (Close_Price['time'] < end)][['time', 'diff']]
    os.chdir(lsy_xps.path_middle + "//" + "Future_Return_Diff")
    Close_Price.to_csv(pair[0 ] +" and  " +pair[1 ]+ " " + str(period) + " ret diff.csv")