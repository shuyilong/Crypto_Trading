import Load_Data as LD
import Global_Variables as GV

function_map = GV.feature_function_map()
def monthly_feature_cal():
    for function, args in function_map.items():
        feature = LD.Load_Feature_Data([function] * len(args), args)
