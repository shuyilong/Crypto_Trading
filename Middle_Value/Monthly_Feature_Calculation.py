import Global_Variables as GV
import os
import Feature

def feature_examize(function_list, arg_list):
    ###############################################################################
    ### INPUT : 1) function_list, e.g ['best_bid_diff','best_ask_diff']
    ###         2) arg_list, [("BTC", 300), ("BTC", 300)]
    ###############################################################################
    Path = os.path.join(GV.path_middle(), "Features")
    for function, args in zip(function_list, arg_list):
        args_chain = '_'.join(str(arg) for arg in args)
        file_path = os.path.join(Path, function, f"{args_chain}.csv")
        if os.path.exists(file_path):
            continue
        else:
            getattr(Feature, function)(*args)
            continue

function_map = GV.feature_function_map()
def monthly_feature_cal():
    for function, args in function_map.items():
        feature = feature_examize([function] * len(args), args)
