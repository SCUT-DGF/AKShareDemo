import random
import json
import akshare as ak
import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.feature_selection import RFE, SelectKBest, mutual_info_regression
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

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
f = all_stock_data.drop(columns=['日期'])
print(f)

# 将收盘价作为标签
f['收盘价'] = f['收盘']
# f = f.drop(columns=['收盘'])
# print(f)

# 数据预处理 特征标准化
f['收盘价'] = f['收盘价'].shift(-1)
print(f)

f = f[:-1]
y = f['收盘价']
X = f.drop(columns=['收盘价'])
print(len(X))
print(len(y))

sca_x = StandardScaler()
sca_y = StandardScaler()
X_scaled = sca_x.fit_transform(X)
y_scaled = sca_y.fit_transform(y.values.reshape(-1, 1)).ravel()
# print(X_scaled,y_scaled)

# 使用原始特征列名
original_feature_columns = X.columns
print(original_feature_columns)
# 1、
# # 使用RFE选择特征
# k = 5  # 选择前5个特征
# estimator = LinearRegression()
# rfe = RFE(estimator, n_features_to_select=k)
# rfe.fit(X_scaled, y)  # 使用标准化的特征和目标变量
#
# # 使用布尔索引来获取选中的特征的列名
# selected_features = original_feature_columns[rfe.support_]
# print("Selected features:", selected_features)

# 2、
# 使用卡方检验选择特征
k = 5  # 选择前5个特征
selector = SelectKBest(score_func=mutual_info_regression, k=k)
X_new = selector.fit_transform(X_scaled, y)

# 打印选中的特征的列名
selected_features = original_feature_columns[selector.get_support(indices=True)]
print("Selected features:", selected_features)

# 划分训练集和测试集
X_train, X_test, y_train, y_test = train_test_split(X_new, y_scaled, test_size=0.1, random_state=42)
# print(X_test)

# 创建线性回归模型
lr_model = LinearRegression()

# 训练模型
lr_model.fit(X_train, y_train)

# 预测测试集
y_pred = lr_model.predict(X_test)

# 反标准化，将预测值转换回原始的价格范围
y_pred_original = sca_y.inverse_transform(y_pred.reshape(-1, 1)).ravel()

# 评估模型
mse = mean_squared_error(sca_y.inverse_transform(y_test.reshape(-1, 1)), y_pred_original)
print(f'Mean Squared Error: {mse}')

# 显示模型的系数
print(f'Coefficients: {lr_model.coef_}')


import matplotlib.pyplot as plt
# 使用 sca_y.inverse_transform 来获取原始尺度的真实值
y_test_original = sca_y.inverse_transform(y_test.reshape(-1, 1)).ravel()

# 绘制真实值和预测值的曲线图
plt.figure(figsize=(10, 6))  # 设置图形的大小
plt.plot(y_test_original, label='Actual Prices')  # 绘制真实值
plt.plot(y_pred_original, label='Predicted Prices')  # 绘制预测值

# 添加图例
plt.legend()

# 添加标题和轴标签
plt.title('Stock Price Prediction')
plt.xlabel('Index')
plt.ylabel('Price')

# 优化x轴的显示，使其按照实际的索引值显示
plt.xticks(np.arange(len(y_test_original)))

# 显示图形
plt.show()


