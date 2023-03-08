import os
import pandas as pd
import Global_Variables as GV
from functools import lru_cache
import Feature

@lru_cache()
def load_feature_daily_data(function, args):
    Path = os.path.join(GV.path_middle(), "Features")
    args_chain = '_'.join(str(arg) for arg in args)
    file_path = os.path.join(Path, function, f"{args_chain}.csv")
    if os.path.exists(file_path):
        file = pd.read_csv(file_path)[['second_timestamp', 'feature']]
        file.columns = ['second_timestamp', function + "_" + args_chain]
        return file
    else:
        getattr(Feature, function)(*args)
        file = pd.read_csv(file_path)[['second_timestamp', 'feature']]
        file.columns = ['second_timestamp', function + "_" + args_chain]
        return file
