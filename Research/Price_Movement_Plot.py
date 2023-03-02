import os
import Data_Clean as DC
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from Global_Variables import path_global

def Price_Movement_Plot(currency_list, begin_date, end_date, freq):
    ###############################################################################
    ### This function is for visualization one pair relationship in specified date range;
    ### INPUT : 1) currency_list, currencies you want to display, e.g: ["BTC","ETH"]
    ###         2) begin_date, start date you want to display (data includes this date)
    ###                        e.g : "2022-10-10"
    ###         3) end_date, end date you want to display (data includes this date)
    ###                        e.g : "2022-10-10"
    ###         4) freq, The frequency of the data you want to display, in seconds
    ### OUTPUT : A graph shows the trade price movement of 2 currency in specify frequency
    ###############################################################################
    ### Plz modify this path to middle value path
    os.chdir(path_global.path_middle +"// " +"Trade_Second_Data")
    ### Save all currency data
    Close_Price = []
    for currency in currency_list:
        close_price = pd.read_csv(currency +"_Second_Data.csv")
        begin, end = DC.Normal_to_Timestamp(begin_date), DC.Normal_to_Timestamp(end_date)
        close_price = close_price[(close_price['timestamp'] >= begin) & \
                                  (close_price['timestamp'] <= end)]
        Close_Price.append(close_price)
    ### dataclean
    time_range = DC.time_interval(Close_Price[0]['time'].iloc[0], \
                                  Close_Price[0]['time'].iloc[-1], 1)
    Multi_Price = pd.DataFrame(time_range, columns = ['time'])
    for i in range(len(Close_Price)):
        Multi_Price = pd.merge(Multi_Price, Close_Price[i][['time' ,'price']], \
                               how='left', on = ['time'])
    column_name = ['time']
    column_name.extend(currency_list)
    Multi_Price.columns = column_name
    Multi_Price = Multi_Price.fillna(method='ffill')
    Multi_Price = Multi_Price.fillna(method='bfill')

    ### Then covert the begin price to 1
    for currency in currency_list:
        Multi_Price[currency] = Multi_Price[currency] / Multi_Price[currency].iloc[0]

    Multi_Price = Multi_Price.iloc[::freq]

    ### Draw price movement graph
    Multi_Price['time'] = pd.to_datetime(Multi_Price['time'])
    Multi_Price.set_index('time', inplace=True)

    sns.set_style('whitegrid')
    fig, ax = plt.subplots()
    sns.lineplot(data=Multi_Price, ax=ax)
    ax.set_xlabel('Time')
    ax.set_ylabel('Price')
    ax.set_title("Price Movement")
    ax.set_xticklabels(ax.get_xticklabels(), rotation=90)
    return plt.show()