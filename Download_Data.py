from tardis_dev import datasets, get_exchange_details
import logging
import nest_asyncio
nest_asyncio.apply()
import os
import gzip
###============================input year/month for data====================###
def un_gz(file_name):
    f_name = file_name.replace(".gz", "")
    g_file = gzip.GzipFile(file_name)
    open(f_name, "wb+").write(g_file.read())
    g_file.close()

from_date = "2022-10-01"
to_date="2023-03-01"
  
### Input Data
symbols_list = ["BTC","ETH","BNB","XRP","BUSD","ADA","DOGE","MATIC","SOL","DOT",\
                "SHIB","LTC","TRX","AVAX","UNI","ATOM","LINK","XMR","ETC",\
                "APT","BCH","XLM","APE","NEAR","FIL","LDO","QNT","ALGO","VET","HBAR","ICP"]

path = "D://Cryptocurrency Data//Spot"
exchange="binance"
data_types_list=["book_snapshot_25","trades","incremental_book_L2","quotes"]
###=======================Get snapshot and trade Data=======================###
# comment out to disable debug logs
logging.basicConfig(level=logging.DEBUG)
for dtype in data_types_list:
    for symbols in symbols_list:
        ### Set up folder
        data_types=[dtype]   # 2 choices: trades / book_snapshot_25
        final_path = path + "//"+dtype+"//"+symbols
        if not os.path.exists(final_path):
            os.makedirs(final_path) 
        os.chdir(final_path)
        ### download data
        try:
            datasets.download(
                # one of https://api.tardis.dev/v1/exchanges with supportsDatasets:true - use 'id' value
                exchange=exchange,
                # accepted data types - 'datasets.symbols[].dataTypes' field in https://api.tardis.dev/v1/exchanges/deribit,
                # or get those values from 'deribit_details["datasets"]["symbols][]["dataTypes"] dict above
                data_types=[dtype],   ## "book_snapshot_25" or "trades"
                # change date ranges as needed to fetch full month or year for example
                from_date=from_date,
                # to date is non inclusive
                to_date=to_date,
                # accepted values: 'datasets.symbols[].id' field in https://api.tardis.dev/v1/exchanges/deribit
                #symbols=[symbols+"-PERPETUAL"],
                symbols=[symbols+"USDT"],
                # (optional) your API key to get access to non sample data as well
                api_key="TD.iurvoO4VgXGJkGWT.nFdpuQjjU15NszS.j6s-dOsCdUISY6J.nrrtT9X9j98VIto.5YHKD8Z0TmJpiq0.MTOI",
                # (optional) path where data will be downloaded into, default dir is './datasets'
                download_dir=final_path,
                # (optional) - one can customize downloaded file name/path (flat dir strucure, or nested etc) - by default function 'default_file_name' is used
                #get_filename=[data_types[0]+"_"+symbols],
                # (optional) file_name_nested will download data to nested directory structure (split by exchange and data type)
                # get_filename=file_name_nested,
                )
        except:
            continue


'''

for symbols in symbols_list:
    os.chdir(path + "//"+symbols)
    for file in os.listdir(final_path):
        if file[-2:] == 'gz':
            un_gz(file)
            os.remove(file)


'''
   

###==============================Get future Data============================###
path = "D://Cryptocurrency Data//Future"
exchange_list={"bitmex":"XBTUSD", "binance-futures":"BTCUSDT", "deribit":"BTC-PERPETUAL"}

data_types_list=["book_snapshot_25","trades","incremental_book_L2","quotes"]

for exchange in exchange_list.keys():
    for data_type in data_types_list:
        final_path = path + "//"+exchange+"//"+data_type
        if not os.path.exists(final_path):
            os.makedirs(final_path) 
        try:
            datasets.download(
                # one of https://api.tardis.dev/v1/exchanges with supportsDatasets:true - use 'id' value
                exchange=exchange,
                # accepted data types - 'datasets.symbols[].dataTypes' field in https://api.tardis.dev/v1/exchanges/deribit,
                # or get those values from 'deribit_details["datasets"]["symbols][]["dataTypes"] dict above
                data_types=[data_type],   ## "derivative_ticker" or "options_chain"
                # change date ranges as needed to fetch full month or year for example
                from_date=from_date,
                # to date is non inclusive
                to_date=to_date,
                # accepted values: 'datasets.symbols[].id' field in https://api.tardis.dev/v1/exchanges/deribit
                #symbols=[symbols+"-PERPETUAL"],
                symbols=[exchange_list[exchange]],
                # (optional) your API key to get access to non sample data as well
                api_key="TD.iurvoO4VgXGJkGWT.nFdpuQjjU15NszS.j6s-dOsCdUISY6J.nrrtT9X9j98VIto.5YHKD8Z0TmJpiq0.MTOI",
                # (optional) path where data will be downloaded into, default dir is './datasets'
                download_dir=final_path,
                # (optional) - one can customize downloaded file name/path (flat dir strucure, or nested etc) - by default function 'default_file_name' is used
                #get_filename=[data_types[0]+"_"+symbols],
                # (optional) file_name_nested will download data to nested directory structure (split by exchange and data type)
                # get_filename=file_name_nested,
                )
        except:
            continue





###==============================Get Option Data============================###
path = "D://Cryptocurrency Data//Option"
#exchange_list={"deribit":"OPTIONS", "okex-options":"OPTIONS"}
exchange_list={"deribit":"OPTIONS"}

data_types_list=["book_snapshot_25","trades","options_chain","quotes"]

for exchange in exchange_list.keys():
    for data_type in data_types_list:
        final_path = path + "//"+exchange+"//"+data_type
        if not os.path.exists(final_path):
            os.makedirs(final_path) 
        try:
            datasets.download(
                # one of https://api.tardis.dev/v1/exchanges with supportsDatasets:true - use 'id' value
                exchange=exchange,
                # accepted data types - 'datasets.symbols[].dataTypes' field in https://api.tardis.dev/v1/exchanges/deribit,
                # or get those values from 'deribit_details["datasets"]["symbols][]["dataTypes"] dict above
                data_types=[data_type],   ## "derivative_ticker" or "options_chain"
                # change date ranges as needed to fetch full month or year for example
                from_date=from_date,
                # to date is non inclusive
                to_date=to_date,
                # accepted values: 'datasets.symbols[].id' field in https://api.tardis.dev/v1/exchanges/deribit
                #symbols=[symbols+"-PERPETUAL"],
                symbols=[exchange_list[exchange]],
                # (optional) your API key to get access to non sample data as well
                api_key="TD.iurvoO4VgXGJkGWT.nFdpuQjjU15NszS.j6s-dOsCdUISY6J.nrrtT9X9j98VIto.5YHKD8Z0TmJpiq0.MTOI",
                # (optional) path where data will be downloaded into, default dir is './datasets'
                download_dir=final_path,
                # (optional) - one can customize downloaded file name/path (flat dir strucure, or nested etc) - by default function 'default_file_name' is used
                #get_filename=[data_types[0]+"_"+symbols],
                # (optional) file_name_nested will download data to nested directory structure (split by exchange and data type)
                # get_filename=file_name_nested,
                )
        except:
            continue



###==============================Get Swap Data=============================###
path = "D://Cryptocurrency Data//Swap"
exchange_list={"huobi-dm-swap":"BTC-USD", "okex-swap":"BTC-USD-SWAP"}

data_types_list=["book_snapshot_25","trades","quotes", "incremental_book_L2"]

for exchange in exchange_list.keys():
    for data_type in data_types_list:
        final_path = path + "//"+exchange+"//"+data_type
        if not os.path.exists(final_path):
            os.makedirs(final_path) 
        try:
            datasets.download(
                # one of https://api.tardis.dev/v1/exchanges with supportsDatasets:true - use 'id' value
                exchange=exchange,
                # accepted data types - 'datasets.symbols[].dataTypes' field in https://api.tardis.dev/v1/exchanges/deribit,
                # or get those values from 'deribit_details["datasets"]["symbols][]["dataTypes"] dict above
                data_types=[data_type],   ## "derivative_ticker" or "options_chain"
                # change date ranges as needed to fetch full month or year for example
                from_date=from_date,
                # to date is non inclusive
                to_date=to_date,
                # accepted values: 'datasets.symbols[].id' field in https://api.tardis.dev/v1/exchanges/deribit
                #symbols=[symbols+"-PERPETUAL"],
                symbols=[exchange_list[exchange]],
                # (optional) your API key to get access to non sample data as well
                api_key="TD.iurvoO4VgXGJkGWT.nFdpuQjjU15NszS.j6s-dOsCdUISY6J.nrrtT9X9j98VIto.5YHKD8Z0TmJpiq0.MTOI",
                # (optional) path where data will be downloaded into, default dir is './datasets'
                download_dir=final_path,
                # (optional) - one can customize downloaded file name/path (flat dir strucure, or nested etc) - by default function 'default_file_name' is used
                #get_filename=[data_types[0]+"_"+symbols],
                # (optional) file_name_nested will download data to nested directory structure (split by exchange and data type)
                # get_filename=file_name_nested,
                )
        except:
            continue






  