import os
import re
import pandas as pd
import Data_Clean as DC
from Global_Variables import path_global
import multiprocessing as mp
from Multi_Processing import Middle_Second_Data_Use
from tqdm import tqdm

def middle_second_data():
    ###############################################################################
    ### This function is for calculating trade second data;
    ###############################################################################
    Path = path_global.path_spot() + "//binance//book_snapshot_25"
    currency_list = os.listdir(Path)
    Date_Range = DC.get_date_range(path_global.begin_date(),path_global.end_date())
    os.chdir(Path + "//" + currency_list[0])
    match = re.search(r"\d{4}-\d{2}-\d{2}", os.listdir()[0])
    before, after = os.listdir()[0][:match.start()], os.listdir()[0][-11:]
    for currency in currency_list:
        print(currency)
        os.chdir(Path + "//" + currency)

        cpu_num = 32
        Final_Result_List = []
        for i in range(len(Date_Range) // cpu_num + (len(Date_Range) % cpu_num > 0)):
            Final_Result = pd.DataFrame()
            date_range = Date_Range[i * cpu_num: (1 + i) * cpu_num]
            pool = mp.Pool(processes=cpu_num)
            results = [pool.apply_async(Middle_Second_Data_Use.process_data, args=(currency, date, \
                            before, after)) for date in date_range]
            for result in tqdm(results):
                Final_Result = pd.concat([Final_Result, result.get()])
            Final_Result_List.append(Final_Result)
            pool.close()
            pool.join()

        Final_Result_List = pd.concat(Final_Result_List)
        Final_Result_List.index = range(len(Final_Result_List))
        file_path = path_global.path_middle()
        if not os.path.exists(path_global.path_middle() + '//Middle_Second_Data'):
            os.makedirs(path_global.path_middle() + '//Middle_Second_Data')
        os.chdir(path_global.path_middle() + '//Middle_Second_Data')
        Final_Result_List.to_csv(currency + "_Second_Data.csv")

