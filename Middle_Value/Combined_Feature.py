import os
import Global_Variables as GV
import Data_Clean as DC
import multiprocessing as mp
from Multi_Processing import Combined_Feature_Use

def Feature_Combine(symbol, date_range=DC.get_date_range("2022-10-02", "2022-10-31")):
    Path = os.path.join(GV.path_middle(), "Features")
    Feature_List = os.listdir(Path)

    pool = mp.Pool(processes=32)
    args_list = [(symbol, date_range, date, Feature_List) for date in date_range]
    pool.starmap(Combined_Feature_Use.process_data, args_list)
    pool.close()
    pool.join()