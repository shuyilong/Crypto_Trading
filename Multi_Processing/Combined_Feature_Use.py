import pandas as pd
import os
import Global_Variables as GV
from tqdm import tqdm

Path = os.path.join(GV.path_middle(), "Features")
def process_data(symbol, date_range, date, Feature_List):
    for feature in Feature_List:
        print(feature)
        Feature_Data = pd.date_range(start=date + " 00:00:00", end=date + " 23:59:59", \
                                     freq='1s').astype(int) // 10 ** 9
        Feature_Data = pd.DataFrame({"second_timestamp": Feature_Data})
        File_List = [file for file in os.listdir(os.path.join(Path, feature)) if file.split("_")[0] == symbol \
                     and date_range[0] in file]
        for file in tqdm(File_List):
            data = pd.read_csv(os.path.join(Path, feature, file))[['second_timestamp', 'feature']]
            data.columns = ['second_timestamp', feature + '_' + file.split('_2022')[0]]
            Feature_Data = pd.merge(Feature_Data, data, how="left", on="second_timestamp")

        if os.path.exists(os.path.join(GV.path_combined_features(), feature)):
            os.chdir(os.path.join(GV.path_combined_features(), feature))
            Feature_Data.to_csv(f"{symbol}_{feature}_{date}.csv")
        else:
            os.makedirs(os.path.join(GV.path_combined_features(), feature))
            os.chdir(os.path.join(GV.path_combined_features(), feature))
            Feature_Data.to_csv(f"{symbol}_{feature}_{date}.csv")