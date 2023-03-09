from itertools import product
from itertools import combinations

symbols = ["BTC", "ETH"]
period = [15, 60, 300, 600]
order_direction1 = ["ask", "bid"]
order_direction2 = ["ask", "bid", "both"]
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
combinations = [(sym, short, long, st, en) for sym, short, long, st, en in \
                product(symbols, period, period, start, end) if short < long]


def feature_function_map():
    Map = {'best_diff' : list(product(symbols, period, order_direction1, start, end)),\
           'n_depth' : list(product(symbols, period, n, data_type1, order_direction1, start, end)),\
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
           'diff_middle_ma' : [(sym, short, long, st, en) for sym, short, long, st, en in \
                product(symbols, period, period, start, end) if short < long], \
           'diff_middle_ema' : [(sym, short, long, st, en) for sym, short, long, st, en in \
                product(symbols, period, period, start, end) if short < long], \
           'slow_sto' : list(product(symbols, period, start, end)),\
           'rsi' : list(product(symbols, period, start, end)),\
           'disparity' : list(product(symbols, period, start, end)),\
           'order_volumn' : list(product(symbols, period, data_type4, order_direction2, start, end)),\
           'order_volumn_imbalance' : list(product(symbols, period, start, end)),\
           'order_frequency' : list(product(symbols, period, data_type4, order_direction2, start, end)),\
           'order_frequency_imbalance' : list(product(symbols, period, start, end)),\
           'order_volumn_derivative' : list(product(symbols, period, order_direction2, start, end)),\
           'order_frequency_derivative' : list(product(symbols, period, order_direction2, start, end)),\
           'order_volumn_derivative_2nd' : list(product(symbols, period, order_direction2, start, end)),\
           'order_frequency_derivative_2nd' : list(product(symbols, period, order_direction2, start, end)),\
           }
    return Map