import Load_Data as LD
import Global_Variables as GV
import os
os.chdir(GV.path_middle() + "//Features")

#####################################################################################################
### 1. Load Data : Features and Price Diff
price_diff = LD.Load_Future_Return_Diff_Data(['BTC','ETH'], 300, start="2022-10-02",end="2022-10-31")
#Feature_List = list(GV.feature_function_map().keys())

os.chdir(GV.path_combined_features())
Feature_List =os.listdir()
Feature_Data = LD.Load_Feature_Data("BTC", "2022-10-02", Feature_List)