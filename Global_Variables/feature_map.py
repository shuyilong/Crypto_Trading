import Feature
from itertools import product

symbols = ["BTC", "ETH"]
time_range = [10, 30, 60, 300, 600]
order_dir = ["ask", "bid"]
start = ["2022-10-02"]
end = ["2022-10-02"]

def feature_function_map():
    Map = {'best_diff' : list(product(symbols, time_range, order_dir)),\
           }
    return Map