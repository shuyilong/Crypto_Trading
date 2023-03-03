import os
import pandas as pd
import Data_Clean as DC
from Global_Variables import path_global
import re
from Multi_Processing import Spot_Snapshot_Second_Data_Use
import multiprocessing as mp
from tqdm import tqdm

def spot_snapshot_second_data():
    Path = path_global.path_spot() + "//binance//book_snapshot_25"
    currency_list = os.listdir(Path)
    for currency in currency_list:
        os.chdir(Path + "//" + currency)
        date_range = [re.findall(r"\d{4}-\d{2}-\d{2}", file)[0] for file in os.listdir()]
        match = re.search(r"\d{4}-\d{2}-\d{2}", os.listdir()[0])
        before, after = os.listdir()[0][:match.start()], os.listdir()[0][match.end():]
        Second_Data = pd.DataFrame()

        pool = mp.Pool(processes=mp.cpu_count())
        results = [pool.apply_async(Spot_Snapshot_Second_Data_Use.process_date, args=(date,)) \
                   for date in date_range]

        for result in tqdm(results, total=len(date_range)):
            Second_Data = pd.concat([Second_Data, result.get()])
        Second_Data.index = range(len(Second_Data))

        file_path = path_global.path_middle()
        if not os.path.exists(file_path + '//Spot_Snapshot_Second_Data'):
            os.makedirs(file_path + '//Spot_Snapshot_Second_Data')
        os.chdir(file_path + '//Spot_Snapshot_Second_Data')
        Second_Data.to_csv(currency + "_Second_Data.csv")
