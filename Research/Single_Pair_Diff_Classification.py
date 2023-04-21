import random
from tqdm import tqdm
import pandas as pd
import Data_Clean as DC
import Load_Data as LD
import Global_Variables as GV
import os
import numpy as np
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Dropout, BatchNormalization

#####################################################################################################
### 1. Load Data : Features and Price Diff
data = LD.Load_Future_Return_Diff_Data(['BTC','ETH'], 300)[['second_timestamp', 'ret_diff']]
data['label'] = data['ret_diff'].apply(lambda x: 1 if abs(x) > 0.001 else 0)

data = LD.Load_Future_Single_Return_Data('ETH',150)
data['label'] = data['period_ret'].apply(lambda x: 1 if abs(x) > 0.001 else 0)

#Feature_List = list(GV.feature_function_map().keys())

os.chdir(GV.path_combined_features())
Feature_List =os.listdir()
Feature_Data_1 = LD.Load_Feature_Data("BTC", "2022-10-02", Feature_List)
Feature_Data_2 = LD.Load_Feature_Data("ETH", "2022-10-02", Feature_List)
for date in tqdm(DC.get_date_range("2022-10-03","2022-10-31")):
    Feature_Data_1 = pd.concat([Feature_Data_1, LD.Load_Feature_Data("BTC", date, Feature_List)])
    Feature_Data_2 = pd.concat([Feature_Data_2, LD.Load_Feature_Data("ETH", date, Feature_List)])

### 2. Combine Data
Whole_Data = pd.merge(data[['second_timestamp', 'label']], Feature_Data_1, how='left', on='second_timestamp')
Whole_Data = pd.merge(Whole_Data, Feature_Data_2, how='left', on='second_timestamp')
Whole_Data = Whole_Data.dropna()

### 3. Drop col with corr > 0.8
corr_matrix = Whole_Data.iloc[:, 2:].corr()
upper_tri = np.triu(corr_matrix, k=1)

for i in range(len(upper_tri)):
    for j in range(i+1, len(upper_tri)):
        if abs(upper_tri[i,j]) > 0.8:
            drop_label = np.random.choice([i,j])
            drop_col = corr_matrix.columns[drop_label]
            if drop_col in Whole_Data.columns:
                del Whole_Data[drop_col]

### 4. Data preprocessing
# delete inf data
for col in Whole_Data.columns[2:]:
    col_max = Whole_Data[col][~np.isinf(Whole_Data[col])].max() # 每列除了 inf 的最大值
    col_min = Whole_Data[col][~np.isinf(Whole_Data[col])].min() # 每列除了 -inf 的最小值
    if col_min == col_max:
        print(col)
        break
    Whole_Data[col] = np.where(Whole_Data[col] == np.inf, col_max, Whole_Data[col])
    Whole_Data[col] = np.where(Whole_Data[col] == -np.inf, col_min, Whole_Data[col])
    Whole_Data[col] = (Whole_Data[col] - col_min) / (col_max - col_min)

### 5. Split Data into train and test
Train_X = Whole_Data[Whole_Data['second_timestamp'] < DC.Normal_to_Timestamp('2022-10-21 00:00:00') // 1000000].iloc[:,2:]
Train_Y = Whole_Data[Whole_Data['second_timestamp'] < DC.Normal_to_Timestamp('2022-10-21 00:00:00') // 1000000].iloc[:,1]
Test_X = Whole_Data[Whole_Data['second_timestamp'] >= DC.Normal_to_Timestamp('2022-10-21 00:00:00') // 1000000].iloc[:,2:]
Test_Y = Whole_Data[Whole_Data['second_timestamp'] >= DC.Normal_to_Timestamp('2022-10-21 00:00:00') // 1000000].iloc[:,1]

Train_X = Train_X.to_numpy()
Train_Y = Train_Y.to_numpy()
Test_X = Test_X.to_numpy()
Test_Y = Test_Y.to_numpy()

### 6. Construct Model
from sklearn.metrics import confusion_matrix
# 找到标签为1的样本和标签为0的样本
ones = np.where(Train_Y == 1)[0]
zeros = np.where(Train_Y == 0)[0]

# 定义神经网络模型
model = tf.keras.Sequential([
    tf.keras.layers.Dense(64, activation='relu', input_dim=Train_X.shape[1]),
    tf.keras.layers.Dense(128, activation='relu'),  # 新增的隐藏层
    tf.keras.layers.Dense(1, activation='sigmoid')
])

# 编译模型
model.compile(optimizer='adam', loss='binary_crossentropy', metrics=['accuracy'])
model.fit(Train_X, Train_Y, epochs=10, batch_size=32, verbose=0,class_weight={0:1,1:1.5})
Pred_Y = model.predict(Test_X)
Pred_Y = (Pred_Y > 0.5).astype(int)
tn, fp, fn, tp = confusion_matrix(Test_Y, Pred_Y).ravel()

# 计算预测为1的情况下真实的1有多少
print(tp / (tp + fp))
print(tp + fp)

