import Feature
from itertools import product

symbols = ["BTC", "ETH"]
time_range = [10, 30, 60, 300, 600]
order_dir = ["ask", "bid"]


def feature_function_map():
    Map = {'best_diff' : list(product(symbols, time_range, order_dir)),\
           'bid_n_depth' : Feature.bid_n_depth,\
           'ask_n_depth' : Feature.ask_n_depth,\
           'window_return': Feature.window_return,\
           'spread_return': Feature.spread_return,\
           'snapshot_derivative' : Feature.snapshot_derivative,\
           'semi_std' : Feature.semi_std,\
           'bipower_var' : Feature.bipower_var,\
           'realized_quarticity' : Feature.realized_quarticity,\
           'snapshot_vol_imbalance' : Feature.snapshot_vol_imbalance,\
           }
    return Map