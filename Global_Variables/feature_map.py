from itertools import product

symbols = ["BTC", "ETH"]
period = [15, 60, 300, 600]
order_direction = ["ask", "bid"]
var_direction = ["positive","negative","total"]
trade_direction = ['buy', 'sell', 'both']
start = ["2022-10-02"]
end = ["2022-10-31"]
n = [0, 4, 14]
data_type1 = ["mean", "sum"]
data_type2 = ["mean", "max"]
data_type3 = ['mean', 'std', 'max', 'min']
data_type4 = ["mean", "std", "sum"]
lag = [2,5]

def feature_function_map():
    Map = {'best_diff' : list(product(symbols, period, order_direction, start, end)),\
           'n_depth' : list(product(symbols, period, n, data_type1, order_direction, start, end)),\
           'middle_mom' : list(product(symbols, period, start, end)),\
           'spread_return' : list(product(symbols, period, n, data_type2, start, end)),\
           'middle_derivative' : list(product(symbols, period, start, end)),\
           'middle_derivative_2nd' : list(product(symbols, period, start, end)),\
           'middle_std' : list(product(symbols, period, var_direction, start, end)),\
           'bipower_var' : list(product(symbols, period, lag, start, end)),\
           'realized_quarticity' : list(product(symbols, period, start, end)),\
           'snapshot_vol_imbalance' : list(product(symbols, period, n, data_type3, start, end)),\
           'trade_volumn' : list(product(symbols, period, data_type4, trade_direction, start, end)),\
           'trade_volumn_imbalance' : list(product(symbols, period, start, end)),\
           'trade_frequency' : list(product(symbols, period, data_type4, trade_direction, start, end)),\
           'trade_frequency_imbalance' : list(product(symbols, period, start, end)),\
           'trade_volumn_derivative' : list(product(symbols, period, trade_direction, start, end)),\
           'trade_frequency_derivative' : list(product(symbols, period, trade_direction, start, end)),\
           'trade_volumn_derivative_2nd' : list(product(symbols, period, trade_direction, start, end)),\
           'trade_frequency_derivative_2nd' : list(product(symbols, period, trade_direction, start, end)),\

           }
    return Map