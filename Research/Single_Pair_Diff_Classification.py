import Load_Data as LD
import Global_Variables as GV
import os
os.chdir(GV.path_middle() + "//Features")

#####################################################################################################
### 1. Load Data : Features and Price Diff
price_diff = LD.Load_Future_Return_Diff_Data(['BTC','ETH'], 300)
Feature_List = list(GV.feature_function_map().keys())

