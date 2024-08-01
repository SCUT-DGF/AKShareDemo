import json
import random
import pandas as pd
import pandas_datareader.data as web
import akshare as ak
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
import datetime
import numpy as np
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Dropout
from collections import deque
from tensorflow.keras.callbacks import ModelCheckpoint
from tensorflow.keras.models import load_model
# start = datetime.datetime(2000, 1, 1)
# end = datetime.datetime(2023, 9, 1)

# 定义获取单只股票数据的函数
def get_stock_data(symbol, start_date, end_date):
    f = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_date,
                           end_date=end_date, adjust="hfq")
    f.dropna(inplace=True)  # 删除缺失值
    f.sort_index(inplace=True)  # 按索引排序
    return f

# 读取JSON文件并提取股票代码
def load_stock_symbols(json_file_path):
    with open(json_file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
        stock_symbols = [item['代码'] for item in data]
    return stock_symbols

# 假设JSON文件路径
json_file_path = './stock_data_code/stock_data/company_history_data/sh_a_stocks.json'
stock_symbols = load_stock_symbols(json_file_path)

# 打印所有股票代码
print(stock_symbols)

# 定义日期范围
start_date = "20230101"
end_date = "20231231"

# 随机选择n个股票代码
n = 5  # 假设我们随机选择5个公司
stock_symbols = random.sample(stock_symbols, n)
print("随机选择的股票代码:", stock_symbols)

# 初始化一个空的DataFrame用于存储所有股票数据
all_stock_data = pd.DataFrame()

# 循环获取多只股票数据，并使用pd.concat合并
for symbol in stock_symbols:
    stock_data = get_stock_data(symbol, start_date, end_date)
    all_stock_data = pd.concat([all_stock_data, stock_data], ignore_index=True, sort=False)

# 打印合并后的数据集（去掉日期列）
# f = all_stock_data.drop(columns=['日期'])
f = all_stock_data
print(f)

# f = ak.stock_zh_a_hist(symbol="603005", period="daily", start_date="20000101",end_date="20231231", adjust="hfq")
# print(f)

# 数据预处理
def Stock_Price_LSTM_Data_Precesing(f, mem_hisdays, pre_days):
    f.dropna(inplace=True)
    f.sort_index(inplace=True)
    f['label'] = f['收盘'].shift(-pre_days)
    scaler = StandardScaler()
    stock_data = f.iloc[:, 2:]  # 去掉股票代码和股票日期
    f = stock_data.drop(columns=['收盘'])  # 去掉收盘
    print(f)
    sca_X = scaler.fit_transform(f.iloc[:, :-1])
    # print(len(sca_X))
    deq = deque(maxlen=mem_hisdays)
    X = []
    for i in sca_X:
        deq.append(list(i))
        if len(deq) == mem_hisdays:
            X.append(list(deq))
    X_lately = X[-pre_days:]
    # print(len(X))
    X = X[:-pre_days]
    y = f['label'].values[mem_hisdays - 1:-pre_days]
    # print(y)
    X = np.array(X)
    y = np.array(y)
    print(X)
    print(y)
    return X, y, X_lately

# 得到训练集和测试集
X, y, X_lately = Stock_Price_LSTM_Data_Precesing(f, 5, 10)
# Stock_Price_LSTM_Data_Precesing(f, 5, 10)
print(X,y)

# 需要预测pre_days以后的股票收盘价
pre_days = 10

# 模型优化需要的参数
# mem_days = [5, 10]
# lstm_layers = [1, 2]
# dense_layers = [1, 2]
# units = [16, 32]

# 最终得到的最优模型的参数
mem_days = [5,10]
lstm_layers = [1]
dense_layers = [1]
units = [32]

# 模型优化
# for i in mem_days:
#     for j in lstm_layers:
#         for k in dense_layers:
#             for l in units:
#                 filepath = './models/{val_mape:.2f}_{epoch:02d}' + f'mem_{i}_lstm_{j}_dense_{k}_units_{l}.keras'
#                 checkpoint = ModelCheckpoint(
#                     filepath=filepath,
#                     save_weights_only=False,
#                     monitor = 'val_mape',
#                     mode='min',
#                     save_best_only=True
#                 )
#                 X, y, X_lately = Stock_Price_LSTM_Data_Precesing(f, i, pre_days)
#                 X_train, X_test, y_train, y_test = train_test_split(
#                     X, y, test_size=0.1, shuffle=False
#                 )
#                 model = Sequential()
#                 model.add(LSTM(l, input_shape=X.shape[1:], activation='relu', return_sequences=True))
#                 model.add(Dropout(0.1))
#
#                 for temp in range(j):
#                     model.add(LSTM(l, activation='relu', return_sequences=True))
#                     model.add(Dropout(0.1))
#
#                 model.add(LSTM(l, activation='relu'))
#                 model.add(Dropout(0.1))
#
#                 for temp in range(k):
#                     model.add(Dense(l, activation='relu'))
#                     model.add(Dropout(0.1))
#
#                 model.add(Dense(1))
#
#                 model.compile(
#                     optimizer='adam',
#                     loss='mse',
#                     metrics=['mape']
#                 )
#                 model.fit(X_train, y_train, batch_size=32, epochs=60, validation_data=(X_test, y_test),callbacks=[checkpoint])

# 划分数据集
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.1, shuffle=False
)

# 根据模型优化中找到最优模型
best_model = load_model('./models/7.05_16mem_5_lstm_1_dense_1_units_32.keras')

# 模型预测
pre = best_model.predict(X_test)

# 绘制预测结果
import matplotlib.pyplot as plt

# 得到x轴的日期
# df_time = f.index[-len(y_test):]
df_time = f['日期'][-len(y_test):]
print(df_time)

# 绘图，横轴为日期，纵轴为股票价格
plt.plot(df_time,y_test,color='red',label='price')
plt.plot(df_time,pre,color='blue',label='predict')
plt.show()