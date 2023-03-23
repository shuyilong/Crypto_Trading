import Load_Data as LD
import Global_Variables as GV
import os

#####################################################################################################
### 1. Load Data : Features and Price Diff
price_diff = LD.Load_Future_Return_Diff_Data(['ADA','ALGO'], 300)[['second_timestamp', 'ret_diff']]
#Feature_List = list(GV.feature_function_map().keys())

os.chdir(GV.path_combined_features())
Feature_List =os.listdir()
Feature_Data_1 = LD.Load_Feature_Data("BTC", "2022-10-02", Feature_List)
Feature_Data_2 = LD.Load_Feature_Data("ETH", "2022-10-02", Feature_List)

### 2.
