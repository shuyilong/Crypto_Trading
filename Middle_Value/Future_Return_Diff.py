import os
import pandas as pd
import Data_Clean as DC
import Global_Variables as GV
import Middle_Value as MV

def future_return_diff(pair, interval, start=GV.begin_time(), end = GV.end_time()):
    ###############################################################################
    ### This function is for calculating ret difference between 2 currency;
    ### And you should choose the calculation period of return as freq;
    ### The return here refers to the return of the future period calculated today;
    ### INPUT : 1) pair, 2 currencies you want to display, e.g: ["BTC","ETH"]
    ###         2) interval, period of return calculation, in seconds
    ###         3) start, Start time of statistics
    ###         4) end, End time of statistics
    ###############################################################################
    os.chdir(GV.path_middle_second_data())
    data_1 = MV.future_period_return(pair[0], start, end, interval)
    data_2 = MV.future_period_return(pair[1], start, end, interval)
    time_used = DC.time_interval(start, end, interval, istimestamp=True)

    Return_Diff_Data = pd.DataFrame(time_used,columns=['second_timestamp'])
    Return_Diff_Data = pd.merge(Return_Diff_Data, data_1, how = 'left', on = 'second_timestamp')
    Return_Diff_Data = pd.merge(Return_Diff_Data, data_2, how='left', on='second_timestamp')
    Return_Diff_Data = Return_Diff_Data.fillna(method='ffill')
    Return_Diff_Data.columns = ['second_timestamp', pair[0], pair[1]]
    Return_Diff_Data['ret_diff'] = Return_Diff_Data[pair[0]] - Return_Diff_Data[pair[1]]

    os.chdir(GV.path_middle() + "//" + "Future_Return_Diff")
    Return_Diff_Data[['second_timestamp','ret_diff']].to_csv(f"{pair[0]}_{pair[1]}_{interval}_{start[:10]}_{end[:10]}.csv")