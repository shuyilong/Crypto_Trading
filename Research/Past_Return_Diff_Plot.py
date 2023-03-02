import os
import Data_Clean as DC
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from Global_Variables import path_global


def Past_Return_Diff_Plot(pair, begin_time, end_time, period, freq):
    ###############################################################################
    ### This function is for calculating ret difference between 2 currency;
    ### And you should choose the calculation period of return as freq;
    ### The income here refers to the income of the past period calculated today;
    ### INPUT : 1) pair, 2 currencies you want to display, e.g: ["BTC","ETH"]
    ###         2) begin_time, start date you want to display (data includes this date)
    ###                        e.g : "2022-10-10" (other format accepted)
    ###         3) end_time, end date you want to display (data not includes this date)
    ###                        e.g : "2022-10-10" (other format accepted)
    ###         4) period, period of return calculation, in seconds
    ###         5) freq, Frequency of ploting
    ### OUTPUT : A graph shows the price  differenced movement of 2 currency in specify frequency
    ###############################################################################
    os.chdir(path_global.path_middle + "//" + "Trade_Second_Data")
    data_1 = pd.read_csv(pair[0] +"_Second_Data.csv")
    data_1 = DC.Choose_Period_Data(data_1, begin_time, end_time)
    data_2 = pd.read_csv(pair[1] +"_Second_Data.csv")
    data_2 = DC.Choose_Period_Data(data_2, begin_time, end_time)
    ### Record all price
    time_range = DC.time_interval(data_1['time'].iloc[0][:10 ] +" 08:00:00", \
                                  data_1['time'].iloc[-1][:10 ] +" 07:59:59", 1)
    Close_Price = pd.DataFrame(time_range ,columns = ['time'])
    Close_Price = pd.merge(Close_Price, data_1[['time' ,'price']], how='left', on='time')
    Close_Price = pd.merge(Close_Price, data_2[['time' ,'price']], how='left', on='time')
    column_name = ['time']
    column_name.extend(pair)
    Close_Price.columns = column_name
    Close_Price = Close_Price.fillna(method='ffill')
    Close_Price = Close_Price.fillna(method='bfill')
    ### Calculate return according to the specified frequency
    Close_Price[pair[0]] = Close_Price[pair[0]] / Close_Price[pair[0]].shift(period)
    Close_Price[pair[1]] = Close_Price[pair[1]] / Close_Price[pair[1]].shift(period)
    Close_Price['diff'] = Close_Price[pair[0]] - Close_Price[pair[1]]
    Close_Price = Close_Price.iloc[period: ,:]

    ### Draw price diff graph
    Close_Price = Close_Price.iloc[::freq]
    Max = abs(Close_Price['diff']).max()

    fig, ax = plt.subplots(figsize=(8, 6))
    ax.bar(Close_Price['time'], Close_Price['diff'], color='steelblue', alpha=0.8, width=0.6)
    plt.title('Price Difference Between  ' +pair[0] + " and " + pair[1], fontsize=16)
    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)
    ax.set_ylim([-Max, Max])
    return plt.show()