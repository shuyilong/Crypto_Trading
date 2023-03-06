import Feature

def feature_function_map():
    Map = {'best_bid_diff' : Feature.best_bid_diff,\
           'best_ask_diff' : Feature.best_ask_diff,\
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