import os
import pandas as pd
import Data_Clean as DC
from Global_Variables import path_global
import re
from numba import cuda
import math

Path = path_global.path_spot() + "//binance//book_snapshot_25"
os.chdir(Path + "//ADA")
date_range = [re.findall(r"\d{4}-\d{2}-\d{2}", file)[0] for file in os.listdir()]
match = re.search(r"\d{4}-\d{2}-\d{2}", os.listdir()[0])
before, after = os.listdir()[0][:match.start()], os.listdir()[0][match.end():]

@cuda.jit
def gpu_read(date_range, result, n=149):
    idx = cuda.threadIdx.x + cuda.blockDim.x * cuda.blockIdx.x
    if idx < n :
        target_file = pd.read_csv(before + date_range[idx] + after)
        result[idx] = len(target_file)

gpu_result = cuda.device_array(149)
threads_per_block = 1024
blocks_per_grid = math.ceil(149 / threads_per_block)
gpu_read[blocks_per_grid, threads_per_block](date_range, gpu_result, n=149)
cuda.synchronize()


