import Feature
from itertools import product

symbols = ["BTC", "ETH"]
period = [10, 30, 60, 300, 600]
order_direction = ["ask", "bid"]
start = ["2022-10-02"]
end = ["2022-10-02"]
n = [0, 4, 14]
data_type = ["mean", "sum", "std", "max"]

def feature_function_map():
    Map = {'best_diff' : list(product(symbols, period, order_direction)),\
           'n_depth' : list(product(symbols, period, n, data_type, order_direction)),\
           }
    return Map