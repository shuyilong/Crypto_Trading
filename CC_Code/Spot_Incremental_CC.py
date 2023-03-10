import pandas as pd
import os
import re
from itertools import product
import datetime

##################################################################################
symbol = "ADA"
Path = "/lustre07/scratch/longsy/incremental_book_L2/"
earliest_file = "2022-10-01"
latest_file = "2023-02-21"
date = "2022-10-02"

##################################################################################
symbols = ["BTC", "ETH"]
period = [15, 60, 300, 600]
order_direction2 = ["ask", "bid", "both"]
data_type4 = ["mean", "std", "sum"]
##################################################################################
def feature_function_map():
    Map = {
           'order_volumn' : list(product(symbols, period, data_type4, order_direction2)),\
           'order_volumn_imbalance' : list(product(symbols, period)),\
           'order_frequency' : list(product(symbols, period, data_type4, order_direction2)),\
           'order_frequency_imbalance' : list(product(symbols, period)),\
           'order_volumn_derivative' : list(product(symbols, period, order_direction2)),\
           'order_frequency_derivative' : list(product(symbols, period, order_direction2)),\
           'order_volumn_derivative_2nd' : list(product(symbols, period, order_direction2)),\
           'order_frequency_derivative_2nd' : list(product(symbols, period, order_direction2)),\
           }
    return Map

##################################################################################
def Date_Addtion(date, date_type, num):
    if date_type == "day":
        date_obj = datetime.datetime.strptime(date, "%Y-%m-%d")
        delta = datetime.timedelta(days=num)
        new_date_obj = date_obj + delta
        return new_date_obj.strftime("%Y-%m-%d")
    elif date_type == "hour":
        date_obj = datetime.datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
        delta = datetime.timedelta(hours=num)
        new_date_obj = date_obj + delta
        return new_date_obj.strftime('%Y-%m-%d %H:%M:%S')
    elif date_type == "second":
        date_obj = datetime.datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
        delta = datetime.timedelta(seconds=num)
        new_date_obj = date_obj + delta
        return new_date_obj.strftime('%Y-%m-%d %H:%M:%S')
##################################################################################
def calculate_feature(daily_file, feature_name, args_input, date):
    #########################################################################################
    ### Calculate Global Use Data
    daily_file['volumn'] = daily_file['amount'] * daily_file['price']
    #########################################################################################
    ### Calculate feature data base on feature name
    if feature_name == "order_volumn":
        if args_input['direction'] == "both":
            file = daily_file[['volumn']].groupby(pd.Grouper(freq='1s')).sum()
        else:
            file = daily_file[daily_file['side'] == args_input['direction']][['volumn']].groupby(pd.Grouper(freq='1s')).sum()

        all_seconds = pd.date_range(start=file.index.min(), end=file.index.max(), freq='1s')
        file = file.reindex(all_seconds).fillna(0)

        rolling = file[['volumn']].rolling(window=args_input['period'], min_periods=1)
        func_dict = {"mean": rolling.mean, "median": rolling.median, "max": rolling.max, \
                     "min": rolling.min, "std": rolling.std, "sum": rolling.sum}
        file['feature'] = func_dict[args_input['data_type']]().shift(1)

    elif feature_name == "order_frequency" :
        if args_input['direction'] == "both":
            file = daily_file[['amount']].groupby(pd.Grouper(freq='1s')).count()
        else:
            file = daily_file[daily_file['side'] == args_input['direction']][['amount']].groupby(pd.Grouper(freq='1s')).count()

        all_seconds = pd.date_range(start=file.index.min(), end=file.index.max(), freq='1s')
        file = file.reindex(all_seconds).fillna(0)

        rolling = file[['amount']].rolling(window=args_input['period'], min_periods=1)
        func_dict = {"mean": rolling.mean, "median": rolling.median, "max": rolling.max, \
                     "min": rolling.min, "std": rolling.std, "sum": rolling.sum}
        file['feature'] = func_dict[args_input['data_type']]().shift(1)

    elif feature_name == "order_volumn_imbalance" :
        ask_side = daily_file[daily_file['side'] == 'ask'][['volumn']].groupby(pd.Grouper(freq='1s')).sum()
        bid_side = daily_file[daily_file['side'] == 'bid'][['volumn']].groupby(pd.Grouper(freq='1s')).sum()
        all_seconds = pd.date_range(start=daily_file.index.min(), end=daily_file.index.max(), freq='1s')
        ask_side = ask_side.reindex(all_seconds).fillna(0)
        bid_side = bid_side.reindex(all_seconds).fillna(0)

        ask_side = ask_side[['volumn']].rolling(window=args_input['period'], min_periods=1).sum()
        bid_side = bid_side[['volumn']].rolling(window=args_input['period'], min_periods=1).sum()
        file = (ask_side / bid_side).shift(1).rename(columns={'volumn': 'feature'})

    elif feature_name == "order_frequency_imbalance" :
        ask_side = daily_file[daily_file['side'] == 'ask'][['amount']].groupby(pd.Grouper(freq='1s')).count()
        bid_side = daily_file[daily_file['side'] == 'bid'][['amount']].groupby(pd.Grouper(freq='1s')).count()
        all_seconds = pd.date_range(start=daily_file.index.min(), end=daily_file.index.max(), freq='1s')
        ask_side = ask_side.reindex(all_seconds).fillna(0)
        bid_side = bid_side.reindex(all_seconds).fillna(0)

        ask_side = ask_side[['amount']].rolling(window=args_input['period'], min_periods=1).sum()
        bid_side = bid_side[['amount']].rolling(window=args_input['period'], min_periods=1).sum()
        file = (ask_side / bid_side).shift(1).rename(columns={'amount': 'feature'})

    elif feature_name == "order_volumn_derivative" :
        if args_input['direction'] == "both":
            file = daily_file[['volumn']].groupby(pd.Grouper(freq='1s')).sum()
        else:
            file = daily_file[daily_file['side'] == args_input['direction']][['volumn']].groupby(pd.Grouper(freq='1s')).sum()
        all_seconds = pd.date_range(start=file.index.min(), end=file.index.max(), freq='1s')
        file = file.reindex(all_seconds).fillna(0)

        file['volumn'] = file[['volumn']].rolling(window=args_input['period'], min_periods=1).sum()
        file['feature'] = (file['volumn'] / file['volumn'].shift(args_input['period'])).shift(1)

    elif feature_name == "order_frequency_derivative" :
        if args_input['direction'] == "both":
            file = daily_file[['amount']].groupby(pd.Grouper(freq='1s')).count()
        else:
            file = daily_file[daily_file['side'] == args_input['direction']][['amount']].groupby(pd.Grouper(freq='1s')).count()
        all_seconds = pd.date_range(start=file.index.min(), end=file.index.max(), freq='1s')
        file = file.reindex(all_seconds).fillna(0)

        file['amount'] = file[['amount']].rolling(window=args_input['period'], min_periods=1).sum()
        file['feature'] = (file['amount'] / file['amount'].shift(args_input['period'])).shift(1)

    elif feature_name == "order_volumn_derivative_2nd" :
        if args_input['direction'] == "both":
            file = daily_file[['volumn']].groupby(pd.Grouper(freq='1s')).sum()
        else:
            file = daily_file[daily_file['side'] == args_input['direction']][['volumn']].groupby(pd.Grouper(freq='1s')).sum()
        all_seconds = pd.date_range(start=file.index.min(), end=file.index.max(), freq='1s')
        file = file.reindex(all_seconds).fillna(0)

        file['volumn'] = file[['volumn']].rolling(window=args_input['period'], min_periods=1).sum()
        file['feature'] = ((file['volumn'] + file['volumn'].shift(2 * args_input['period']) - \
                            2 * file['volumn'].shift(args_input['period'])) / file['volumn']).shift(1)

    elif feature_name == "order_frequency_derivative_2nd" :
        if args_input['direction'] == "both":
            file = daily_file[['amount']].groupby(pd.Grouper(freq='1s')).count()
        else:
            file = daily_file[daily_file['side'] == args_input['direction']][['amount']].groupby(pd.Grouper(freq='1s')).count()
        all_seconds = pd.date_range(start=file.index.min(), end=file.index.max(), freq='1s')
        file = file.reindex(all_seconds).fillna(0)

        file['amount'] = file[['amount']].rolling(window=args_input['period'], min_periods=1).sum()
        file['feature'] = ((file['amount'] - 2 * file['amount'].shift(args_input['period']) + \
                            file['amount'].shift(2 * args_input['period'])) / file['amount']).shift(1)
    else:
        raise ValueError("No such feature pre-defined")

    file['second_timestamp'] = pd.DatetimeIndex(file.index).astype(int) // 10 ** 9
    start_time = pd.Timestamp(date + ' 00:00:00')
    end_time = pd.Timestamp(date + ' 23:59:59')
    mask = file.index.slice_indexer(start_time, end_time)
    file = file[['second_timestamp','feature']].iloc[mask]

    name = [str(val) for val in args_input.values()]
    name = '_'.join(name)
    file = file.rename(columns={'feature': feature_name+"_"+name})
    return file

##################################################################################
def read_file(symbol, date, Path=Path, earliest_file=earliest_file, latest_file=latest_file):
    '''
    This function will read file around date
    '''
    file_path = os.path.join(Path, symbol)
    first_file_name = os.listdir(file_path)[0]
    match = re.search(r"\d{4}-\d{2}-\d{2}", first_file_name)
    before, after = first_file_name[:match.start()], first_file_name[match.end():]
    os.chdir(file_path)
    if date == earliest_file:
        file_read = [date, Date_Addtion(date, "day", 1)]
    elif date == latest_file:
        file_read = [Date_Addtion(date, "day", -1), date]
    else:
        file_read = [Date_Addtion(date, "day", -1), date, Date_Addtion(date, "day", 1)]

    file = pd.concat([pd.read_csv(before + target + after) for target in file_read])
    file['second_timestamp'] = file['timestamp'] // 1000000
    file['second_timestamp'] = pd.to_datetime(file['second_timestamp'], unit='s')
    return file.set_index('second_timestamp')

##################################################################################
def order_features(symbol, date):
    daily_file = read_file(symbol, date)[['side', 'price', 'amount']]




order_features(symbol, date)