import random
import json
import akshare as ak
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from sklearn.linear_model import LinearRegression
from sklearn.feature_selection import SelectKBest, f_regression
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler


def get_single_daily_stock_data(symbol, start_date, end_date):
    """
    获取指定股票代码的日行情数据
    :param symbol: 股票代码
    :param start_date: 开始日期
    :param end_date: 结束日期
    :return: DataFrame
    """
    # 获取股票数据
    f = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_date, end_date=end_date, adjust="hfq")
    # 删除缺失值
    f.dropna(inplace=True)
    # 按索引排序
    f.sort_index(inplace=True)
    return f


def load_stock_symbols(json_file_path):
    """
    读取JSON文件并提取股票代码
    :param json_file_path: JSON文件路径
    :return: 股票代码列表
    """
    # 打开JSON文件并加载数据
    with open(json_file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
        stock_symbols = [item['代码'] for item in data]
    return stock_symbols


def get_multiple_daily_stock_data(stock_symbols, start_date, end_date):
    """
    获取多个股票代码的日行情数据，并将他们合并成一个DataFrame
    :param stock_symbols: 股票代码列表
    :param start_date: 开始日期
    :param end_date: 结束日期
    :return: DataFrame
    """
    # 随机选择n个股票代码
    n = 5  # 假设我们随机选择5个公司
    stock_symbols = random.sample(stock_symbols, n)
    # print("随机选择的股票代码:", stock_symbols)

    # 初始化一个空的DataFrame用于存储所有股票数据
    all_stock_data = pd.DataFrame()

    # 循环获取多只股票数据，并使用pd.concat合并
    for symbol in stock_symbols:
        stock_data = get_single_daily_stock_data(symbol, start_date, end_date)
        all_stock_data = pd.concat([all_stock_data, stock_data], ignore_index=True, sort=False)

    return all_stock_data


def preprocess_data(all_stock_data):
    """
    数据预处理
    :param all_stock_data: 合并后的数据集
    :return: 原始特征和标签、标准化后的特征和标签、y的反标准化器、原始特征列名、 日期列
    """
    # 打印合并后的数据集（去掉日期列）
    f_date = all_stock_data['日期']
    f = all_stock_data.drop(columns=['日期'])
    # print(f)

    # 将收盘价作为标签（原本收盘特征可去可不去）
    f['收盘价'] = f['收盘']

    # 将收盘价移动到下一行，作为标签（即预测下一日的收盘价）
    f['收盘价'] = f['收盘价'].shift(-1)
    # print(f)

    # 删除最后一行，因为最后一行的收盘价没有对应的下一日收盘价
    f = f[:-1]

    # 得到特征和标签
    X = f.drop(columns=['收盘价'])
    y = f['收盘价']

    # 标准化特征和标签，得到X_scaled,y_scaled
    sca_x = StandardScaler()
    sca_y = StandardScaler()
    x_scaled = sca_x.fit_transform(X)
    y_scaled = sca_y.fit_transform(y.values.reshape(-1, 1)).ravel()
    # print(x_scaled, y_scaled)

    # 保存原始特征列名
    original_feature_columns = X.columns
    # print(original_feature_columns)
    return X, y, x_scaled, y_scaled, sca_y, original_feature_columns, f_date


def select_best_features(X_scaled, y, original_features, method='chi2', k=5):
    """
    选择最优特征
    :param X_scaled: 标准化后的特征
    :param y: 标签
    :param original_features: 原始特征名称列表
    :param method: 特征选择方法
    :param k: 选择最优特征的数量
    :return: 原始最优特征数据和标准化后的最优特征数据
    """

    if method == 'chi2':
        # 使用f_regression进行特征选择
        selector = SelectKBest(score_func=f_regression, k=k)
        # 训练选择器,得到最优特征的索引
        X_new = selector.fit_transform(X_scaled, y)
        # print(X_new)
        # 根据最优特征的索引，从原始特征名称列表中提取最优特征名称
        selected_features = np.array(original_features)[selector.get_support()]
    else:
         # 如果使用其他方法，请在这里添加相应的代码
         pass
    return pd.DataFrame(X_new, columns=selected_features), X_new


def train(X_new, y_scaled, test_size=0.1, random_state=42):
    """
    训练线性回归模型
    :param X_new: 特征
    :param y_scaled: 标签
    :param test_size: 测试集比例
    :param random_state: 随机种子
    :return: 训练好的模型,测试集特征,测试集标签
    """
    # 划分训练集和测试集
    X_train, X_test, y_train, y_test = train_test_split(X_new, y_scaled, test_size=test_size, random_state=random_state)

    # 创建线性回归模型
    lr_model = LinearRegression()

    # 训练模型
    lr_model.fit(X_train, y_train)

    return lr_model, X_test, y_test


def predict(model, X_test, y_test, sca_y):
    """
    使用训练好的模型进行预测
    :param model: 训练好的模型
    :param X_test: 测试集特征
    :param y_test: 测试集标签
    :param sca_y: y的反标准化器
    :return: 预测结果的反标准化值
    """
    # 使用模型进行预测
    y_pred = model.predict(X_test)

    # 反标准化，将预测值转换回原始的价格范围
    y_pred_original = sca_y.inverse_transform(y_pred.reshape(-1, 1)).ravel()

    # 评估模型
    mse = mean_squared_error(sca_y.inverse_transform(y_test.reshape(-1, 1)), y_pred_original)
    print(f'Mean Squared Error: {mse}')

    # 显示模型的系数
    print(f'Coefficients: {model.coef_}')

    return y_pred_original


def plot_graph(y_test_original, y_pred_original, f_date):
    """
    绘制实际值与预测值的曲线图
    :param y_test_original: 测试集的的反标准化值
    :param y_pred_original: 预测结果的反标准化值
    :param f_date: 日期列
    :return: 无
    """

    print(f_date)
    df_time = f_date[-len(y_test):]
    print(df_time)
    plt.figure(figsize=(10, 6))  # 设置图形的大小
    plt.plot(df_time, y_test_original, label='Actual Prices')  # 绘制真实值
    plt.plot(df_time, y_pred_original, label='Predicted Prices')  # 绘制预测值

    # 添加图例
    plt.legend()

    # 添加标题和轴标签
    plt.title('Stock Price Prediction')
    plt.xlabel('Index')
    plt.ylabel('Price')

    # 优化x轴的显示，使其按照实际的索引值显示
    # 设置x轴的刻度，每10天一个刻度
    plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=20))

    # 设置日期格式为'%Y-%m-%d'
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))

    # 显示图形
    plt.show()

# 主程序
if __name__ == "__main__":

    # JSON文件路径
    json_file_path = './stock_data_code/stock_data/company_history_data/sh_a_stocks.json'

    # 加载股票代码
    stock_symbols = load_stock_symbols(json_file_path)
    # 打印所有股票代码
    # print(stock_symbols)

    # 定义日期范围
    start_date = "20230101"
    end_date = "20231231"

    # 获取股票数据
    f = get_multiple_daily_stock_data(stock_symbols, start_date, end_date)
    # print(f)

    # 数据预处理,得到原始特征和标签、标准化后的特征和标签、y的反标准化器、原始特征列名，日期列
    X, y, x_scaled, y_scaled, sca_y,original_feature_columns, f_date = preprocess_data(f)
    # print(X, y, x_scaled, y_scaled, original_feature_columns)

    # 选择最优特征，得到原始最优特征数据和标准化后的最优特征数据
    X_selected,X_new = select_best_features(x_scaled, y, original_feature_columns, k=5)
    # print(X_selected)
    # print(X_new)

    # 训练模型，得到训练好的模型、测试集特征、测试集标签
    model, X_test, y_test = train(X_new, y_scaled)

    # 预测并得到预测的真实值
    y_pred_original = predict(model, X_test, y_test, sca_y)

    # 得到测试集的真实值
    y_test_original = sca_y.inverse_transform(y_test.reshape(-1, 1)).ravel()
    print(len(y_test_original), len(y_pred_original))

    # 绘制实际值与预测值的曲线图
    plot_graph(y_test_original, y_pred_original, f_date)





