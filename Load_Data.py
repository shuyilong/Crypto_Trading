import os
import pandas as pd
import re
import Middle_Value as MV
import Global_Variables as GV
from tqdm import tqdm
###############################################################################
###############################################################################

def Load_Single_Data_File(product, exchange, data_type, date, symbol=None, Path = GV.path_original()):
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
def Load_Future_Return_Diff_Data(pair, interval, start=GV.begin_time(), end=GV.end_time()):
    ###############################################################################
    ### This function is for loading future return diff;
    ### INPUT : 1) pair, e.g ['BTC','ETH']
    ###         2) period, int in second
    ### OUTPUT : Single file data in DataFrame format
    ###############################################################################
    os.chdir(GV.path_middle() + "//" + "Future_Return_Diff")
    if os.path.exists(f"{pair[0]}_{pair[1]}_{interval}_{start[:10]}_{end[:10]}.csv"):
        return pd.read_csv(f"{pair[0]}_{pair[1]}_{interval}_{start[:10]}_{end[:10]}.csv")
    else:
        MV.future_return_diff(pair, interval, start, end)
        os.chdir(GV.path_middle() + "//" + "Future_Return_Diff")
        return pd.read_csv(f"{pair[0]}_{pair[1]}_{interval}_{start[:10]}_{end[:10]}.csv")

###################################################################################################
def Load_Future_Single_Return_Data(currency, interval, start=GV.begin_time(), end=GV.end_time()):
    ###############################################################################
    ### This function is for loading future return diff;
    ### INPUT : 1) currency, e.g 'BTC','ETH'
    ###         2) interval, int in second
    ### OUTPUT : Single file data in DataFrame format
    ###############################################################################
    os.chdir(GV.path_middle() + "//" + "Single_Future_Return")
    if os.path.exists(f"{currency}_{interval}_{start[:10]}_{end[:10]}.csv"):
        return pd.read_csv(f"{currency}_{interval}_{start[:10]}_{end[:10]}.csv")
    else:
        data = MV.future_period_return(currency, start, end, interval)
        os.chdir(GV.path_middle() + "//" + "Single_Future_Return")
        data.to_csv(f"{currency}_{interval}_{start[:10]}_{end[:10]}.csv")
        return data

###################################################################################################
def Load_Feature_Data(symbol, date, Feature_List):
    ###############################################################################
    ### This function is for loading feature data;
    ### INPUT : 1) function_list, e.g ['best_bid_diff','best_ask_diff']
    ###         2) arg_list, [("BTC", 300), ("BTC", 300)]
    ### OUTPUT : Single file data in DataFrame format
    ###############################################################################
    Path = GV.path_combined_features()
    Feature_Data = pd.DataFrame({"second_timestamp": pd.date_range(start=date+" 00:00:00", \
                                end=date + " 23:59:59", freq='1s').astype(int) // 10**9})
    for feature in Feature_List:
        File_List = os.listdir(os.path.join(Path, feature))
        File = [file for file in File_List if file.split("_")[0] == symbol and \
                     file[-14:-4] == date][0]
        File_Data = pd.read_csv(os.path.join(Path, feature, File)).iloc[:,1:]
        Feature_Data = pd.merge(Feature_Data, File_Data, how='left', on='second_timestamp')
    return Feature_Data



        
        