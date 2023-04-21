from tqdm import tqdm
import pandas as pd
import Data_Clean as DC
import Load_Data as LD
import Global_Variables as GV
import os
import itertools

def action_possibilities(objective_num):
    if objective_num == 1:
        #action_list = list(itertools.product([-1,0,1], [-1,0,1]))
        #reward_list = [(1+ret*k[1]) * (1 - abs(k[1]-k[0]) * GV.cost()) - 1 for k in action_list]
        return [-1,0,1]
    else:
        # TODO
        raise ValueError('haven\'t finished')

def experience_pool(objective, interval, begin_date, end_date):
    if len(objective) == 1:
        os.chdir(GV.path_combined_features())
        Feature_List = os.listdir()
        Feature_Data = LD.Load_Feature_Data(objective[0], begin_date, Feature_List)
        for date in tqdm(DC.get_date_range(DC.Date_Addtion(begin_date,'day',1), end_date)):
            Feature_Data = pd.concat([Feature_Data, LD.Load_Feature_Data(objective[0], date, Feature_List)])

        ret_data = LD.Load_Future_Single_Return_Data(objective[0], interval)
        Whole_Data = pd.merge(ret_data[['second_timestamp','period_ret']], Feature_Data, how='left', on='second_timestamp')
        Whole_Data = Whole_Data[(Whole_Data['second_timestamp'] >= DC.Normal_to_Timestamp(begin_date+" 00:00:00")//1000000) & \
                                (Whole_Data['second_timestamp'] <= DC.Normal_to_Timestamp(end_date + " 23:59:59")//1000000)]
        Whole_Data = Whole_Data.fillna(method='ffill')
        Whole_Data = Whole_Data.fillna(method='bfill')
        Whole_Data = Whole_Data.dropna()

        ### process outlier
        Whole_Data.iloc[:,2:] = Whole_Data.iloc[:,2:].apply(DC.Clean_Outlier)

        ###
        Experience_Pool = pd.DataFrame(columns = ['state', 'last_action', 'reward','next_state'])
        for i in range(len(Whole_Data)-1):
            last_action = action_possibilities(1)
            reward = Whole_Data.iloc[i,1]
            state = list(Whole_Data.iloc[i, 2:])
            next_state = list(Whole_Data.iloc[i+1, 2:])
            new_rows = [{'state':state, 'last_action':last_action[k], 'reward':reward, 'next_state':next_state} for k in range(len(last_action))]
            Experience_Pool = Experience_Pool.append(new_rows, ignore_index=True)
        return Experience_Pool
    else:
        # TODO
        raise ValueError('haven\'t finished')
