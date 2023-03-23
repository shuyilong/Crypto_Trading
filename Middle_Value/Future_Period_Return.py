import os
import pandas as pd
import Data_Clean as DC
import Global_Variables as GV


def future_period_return(currency, begin_time, end_time, interval):
    ###############################################################################
    ### This function is for calculating return in specified time interval for a currency;
    ### Output is a dataframe with 2 cols ['second_timestamp', ['period_ret']
    ### INPUT : 1) currency,  e.g: "BTC"
    ###         2) begin_time, e.g : "2022-10-10 12-20-55"
    ###         3) end_date, e.g : "2022-10-10 22-20-55"
    ###         4) interval, the interval you want to calculate the return. in seconds.
    ###############################################################################
    Path = GV.path_middle_second_data()
    os.chdir(Path)
    data = pd.read_csv(currency + "_Second_Data.csv")[['middle_price', 'second_timestamp']]
    data['middle_price'] = data['middle_price'].shift(1)
    time_used = DC.time_interval(begin_time, end_time, interval, istimestamp=True)

    data = data[data['second_timestamp'].isin(time_used)]
    data['period_ret'] = data['middle_price'].shift(-1) / data['middle_price'] - 1
    return data[['second_timestamp','period_ret']]
