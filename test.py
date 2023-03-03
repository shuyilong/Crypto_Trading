import os
import pandas as pd
import Data_Clean as DC
from Global_Variables import path_global
import re
import multiprocessing as mp
import test2
from tqdm import tqdm

Path = path_global.path_spot() + "//binance//book_snapshot_25"
os.chdir(Path + "//" + "ADA")
date_range = [re.findall(r"\d{4}-\d{2}-\d{2}", file)[0] for file in os.listdir()][:16]
Second_Data = pd.DataFrame()

pool = mp.Pool(processes=mp.cpu_count())
results = []
with tqdm(total=len(date_range)) as pbar:
    for date in date_range:
        results.append(pool.apply_async(test2.process_date, args=(date,)))
        pbar.update()
for result in tqdm(results, total=len(date_range)):
    Second_Data = pd.concat([Second_Data, result.get()])




