import Load_Data as LD
import pandas as pd
import Global_Variables as GV



#####################################################################################################
### 1. Load Data : Features and Price Diff
price_diff = LD.Load_Future_Return_Diff_Data(['BTC','ETH'], 300)
function_map = GV.feature_function_map()

for function, args in function_map.items():
    feature = LD.Load_Feature_Data([function] * len(args), args)
