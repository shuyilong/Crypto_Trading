import Load_Data as LD
import pandas as pd
import Global_Variables as GV



#####################################################################################################
### 1. Load Data : Features and Price Diff
price_diff = LD.Load_Future_Return_Diff_Data(['BTC','ETH'], 300)
#features = LD.Load_Feature_Data(list(GV.feature_function_map().keys())[:2],arg_list=[("BTC",300),("BTC",300)])

