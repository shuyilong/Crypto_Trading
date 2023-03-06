import os
import pandas as pd
import re
import Middle_Value as MV
from Global_Variables import path_global
import Feature
import multiprocessing as mp
import tqdm
###############################################################################
###############################################################################

def Load_Single_Data_File(product, exchange, data_type, date, symbol=None, Path = path_global.path_original()):
###############################################################################
### This function is for loading single data file;
### INPUT : 1) product, choose from "Future"/"Option"/"Spot"/"Swap"
###         2) exchange, choose from "binance"(Spot)/"binance-futures"(Future)/
###                      "bitmex"(Future)/"deribit"(Future, Option)/"huobi-dm-swap"(Swap)/
###                      "okex-swap"(Swap)
###         3) data_type, choose from "book_snapshot_25"(Spot, Future, Optiom, Swap)/
###                      "incremental_book_L2"(Spot, Future, Optiom, Swap)/"liquidations"(Spot)/
###                      "quotes"(Spot, Future, Optiom, Swap)/"trades"(Spot, Future, Optiom, Swap)
###         4) symbol, (this input only for loading Spot data), choose from 
###                      "BTC","ETH","BNB","XRP","BUSD","ADA","DOGE","MATIC","SOL","DOT",
###                      "SHIB","LTC","TRX","AVAX","UNI","ATOM","LINK","XMR","ETC",
###                      "BCH","XLM","APE","NEAR","FIL","LDO","QNT","ALGO","VET","HBAR","ICP"
###         5) date, format of "2022-10-22"
### OUTPUT : Single file data in DataFrame format
###############################################################################
    ### Only Spot data need to specify a symbol
    if product == "Spot":
        if symbol == None:
            raise ValueError("You have to specify a symbol, such as : BTC, ETH")
        else:
            os.chdir(Path + "//" + product + "//" + exchange + "//" + data_type +\
                     "//" + symbol)
            ### Get a file name other than the date
            example_filename = os.listdir()[0]
            match = re.search(r"\d{4}-\d{2}-\d{2}", example_filename)
            before, after = example_filename[:match.start()], example_filename[match.end():]
            target_file = before + date + after
            data = pd.read_csv(target_file)
    
    ### The data format is the same except Spot
    else:
        os.chdir(Path + "//" + product + "//" + exchange + "//" + data_type)
        ### Get a file name other than the date
        example_filename = os.listdir()[0]
        match = re.search(r"\d{4}-\d{2}-\d{2}", example_filename)
        before, after = example_filename[:match.start()], example_filename[match.end():]
        target_file = before + date + after
        data = pd.read_csv(target_file)
    return data

###################################################################################################
def Load_Future_Return_Diff_Data(pair, period):
    ###############################################################################
    ### This function is for loading future return diff;
    ### INPUT : 1) pair, e.g ['BTC','ETH']
    ###         2) period, int in second
    ### OUTPUT : Single file data in DataFrame format
    ###############################################################################
    os.chdir(path_global.path_middle() + "//" + "Future_Return_Diff")
    if os.path.exists('./'+pair[0]+" and "+pair[1]+ " " + str(period) + " ret diff.csv"):
        return pd.read_csv('./'+pair[0]+" and "+pair[1]+ " " + str(period) + " ret diff.csv")
    else:
        MV.Future_Return_Diff(pair, period)
        return pd.read_csv('./'+pair[0]+" and "+pair[1]+ " " + str(period) + " ret diff.csv")

###################################################################################################
from Multi_Processing import Load_Data_Use
def Load_Feature_Data(function_list, arg_list):
    ###############################################################################
    ### This function is for loading feature data;
    ### INPUT : 1) function_list, e.g ['best_bid_diff','best_ask_diff']
    ###         2) arg_list, [("BTC", 300), ("BTC", 300)]
    ### OUTPUT : Single file data in DataFrame format
    ###############################################################################
    Path = os.path.join(path_global.path_middle(), "Features")
    cpu_num = 32
    File_List = []
    for i in range(len(function_list)) // cpu_num + (len(function_list % cpu_num > 0)):
        function_range = function_list[i*cpu_num : (1+i) * cpu_num]
        arg_range = arg_list[i*cpu_num : (1+i) * cpu_num]
        pool = mp.Pool(processes= cpu_num)
        results = [pool.apply_async(Load_Data_Use.load_feature_data, args=(function, args,)) \
                   for function, args in zip(function_range, arg_range)]
        for result in tqdm(results):
            File_List.append(result.get())
        pool.close()
        pool.join()

    Feature_Data = File_List[0]
    for i in range(1,len(File_List)):
        Feature_Data = pd.merge(Feature_Data, File_List[i], how='left', on='second_timestamp')
    return Feature_Data




        
        