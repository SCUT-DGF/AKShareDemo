import datetime
import math
import random
import json
import akshare as ak
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib import style
from sklearn.linear_model import LinearRegression
from sklearn.feature_selection import SelectKBest, f_regression
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
"""
    函数功能：
    get_single_daily_stock_data(symbol, start_date, end_date): 获取指定股票代码的日行情数据
    
    load_stock_symbols(file_path): 读取JSON文件并提取股票代码
    
    get_multiple_daily_stock_data(stock_symbols, start_date, end_date): 获取多个股票代码的日行情数据，并将他们合并成一个DataFrame
    
    preprocess_data(all_stock_data, pre_days): 数据预处理
    
    select_best_features(X, y, original_features, method='chi2', k=5): 特征选择
    
    train(X_best_features, y, test_size=0.1, random_state=42): 训练线性回归模型
    
    predict(model, X_test, y_test): 预测并评估模型
"""

def get_single_daily_stock_data(symbol, start_date, end_date):
    """
    获取指定股票代码的日行情数据
    :param symbol: 股票代码
    :param start_date: 开始日期
    :param end_date: 结束日期
    :return: DataFrame
    """
    # 获取单个上市公司的日行情数据
    single_stock_data = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_date, end_date=end_date, adjust="hfq")

    # 删除缺失值
    single_stock_data.dropna(inplace=True)

    # 按索引排序
    single_stock_data.sort_index(inplace=True)

    # 修改列名为英文
    single_stock_data.rename(columns={
        '日期': 'Date',
        '股票代码': 'Symbol',
        '开盘': 'Open',
        '收盘': 'Close',
        '最高': 'High',
        '最低': 'Low',
        '成交量': 'Volume',
        '成交额': 'Turnover',
        '振幅': 'Amplitude',
        '涨跌幅': 'Change_Percent',
        '涨跌额': 'Change_Amount',
        '换手率': 'Turnover_Rate'
    }, inplace=True)

    # 将日期列转换为datetime类型
    single_stock_data['Date'] = pd.to_datetime(single_stock_data['Date'])

    return single_stock_data

def load_stock_symbols(file_path):
    """
    读取JSON文件并提取股票代码
    :param file_path: JSON文件路径
    :return: 股票代码列表
    """
    # 打开JSON文件并加载数据
    with open(file_path, 'r', encoding='utf-8') as file:
        # 加载数据
        data = json.load(file)

        # 提取股票代码列表
        all_stock_symbols = [item['代码'] for item in data]

    return all_stock_symbols

def get_multiple_daily_stock_data(stock_symbols, start_date, end_date):
    """
    获取多个股票代码的日行情数据，并将他们合并成一个DataFrame
    :param stock_symbols: 股票代码列表
    :param start_date: 开始日期
    :param end_date: 结束日期
    :return: DataFrame
    """
    # 随机选择n个股票代码
    n = 5

    # 得到随机选择的股票代码
    stock_symbols = random.sample(stock_symbols, n)

    # 初始化一个空的DataFrame用于存储所有股票数据
    all_stock_data = pd.DataFrame()

    # 循环获取多只股票数据，并使用pd.concat合并
    for symbol in stock_symbols:
        stock_data = get_single_daily_stock_data(symbol, start_date, end_date)
        all_stock_data = pd.concat([all_stock_data, stock_data], ignore_index=True, sort=False)

    # 计算涨跌幅、最高价与最低价之差与最低价的百分比、收盘价与开盘价之差与开盘价的百分比
    all_stock_data['HL_PCT'] = (all_stock_data['High'] - all_stock_data['Low']) / all_stock_data['Low'] * 100.0
    all_stock_data['PCT_change'] = (all_stock_data['Close'] - all_stock_data['Open']) / all_stock_data['Open'] * 100.0

    return all_stock_data

def preprocess_data(all_stock_data, pre_days):
    """
    数据预处理
    :param all_stock_data: 合并后的数据集
    :param pre_days: 需要预测的天数
    :return: 特征和标签, 预测数据的特征 ,原始特征列名, 日期列, 合并后的数据集（包含标签列）
    """

    # 如果日期列存在并且不是索引列，才进行以下操作
    if 'Date' in all_stock_data.columns and all_stock_data.index.name != 'Date':
        # 保存日期列
        date_col = all_stock_data['Date']
        # 删除日期列
        all_stock_data = all_stock_data.drop(columns=['Date'])

    # 将收盘价作为标签（原本收盘特征可去可不去）
    all_stock_data['Label'] = all_stock_data['Close']

    # 将收盘价移动到下pre_days行，作为标签（即预测下pre_days日的收盘价）
    all_stock_data['Label'] = all_stock_data['Label'].shift(-pre_days)

    # 取出最后pre_days行的数据作为预测
    X_lately = all_stock_data[-pre_days:]

    # 去掉标签列(得到特征)
    X_lately = X_lately.drop(columns=['Label'])

    # 取出前len(all_stock_data)-pre_days行的数据作为训练数据和测试数据
    all_stock_data = all_stock_data[:-pre_days]

    # 保存训练集和测试集
    X = all_stock_data.drop(columns=['Label'])
    y = all_stock_data['Label']

    return X, y, X_lately, all_stock_data

def select_best_features(X, y, method, k):
    """
    特征选择
    :param X: 特征
    :param y: 标签
    :param original_features: 原始特征列名
    :param method: 特征选择方法
    :param k: 选择最优特征的数量
    :return: 最优特征构成的DataFrame, 最优特征名称列表
    """
    # 保存原始特征列名
    original_features = X.columns
    # print(original_features)

    if method == 'chi2':
        # 使用f_regression进行特征选择
        selector = SelectKBest(score_func=f_regression, k=k)

        # 训练选择器,得到最优特征的索引
        X_new = selector.fit_transform(X, y)

        # 根据最优特征的索引，从原始特征名称列表中提取最优特征名称
        selected_features = np.array(original_features)[selector.get_support()]
        # print(selected_features)
    else:
         # 如果使用其他方法，请在这里添加相应的代码
         pass

    return selected_features

def train(X_best_features, X_lately, y, test_size=0.1, random_state=42):
    """
    训练模型
    :param X_best_features: 特征
    :param X_lately: 预测数据的特征
    :param y: 标签
    :param test_size: 测试集比例
    :param random_state: 随机种子
    :return: 训练好的模型, 测试集特征, 测试集标签
    """
    # 划分训练集和测试集
    X_train, X_test, y_train, y_test = train_test_split(X_best_features, y, test_size=test_size, random_state=random_state)

    scaler = StandardScaler()

    # 如果股票代码列存在，才进行以下操作
    if 'Symbol' in X_train.columns:
        # 分离股票代码列
        stock_code_train = X_train['Symbol']

        # 标准化训练集特征（不包括股票代码列）
        X_train_scaled = scaler.fit_transform(X_train.drop(columns=['Symbol']))

        # 将股票代码列重新添加回去
        X_train_scaled = np.hstack((stock_code_train.values.reshape(-1, 1), X_train_scaled))
    else:
        # 标准化训练集特征（包括股票代码列）
        X_train_scaled = scaler.fit_transform(X_train)

    # 创建线性回归模型
    lr_model = LinearRegression()

    # 训练模型
    lr_model.fit(X_train_scaled, y_train)

    return lr_model, X_test, y_test, scaler

def test(model, X_test, y_test):
    """
    预测并评估模型
    :param model: 训练好的模型
    :param X_test: 测试集特征
    :param y_test: 测试集标签
    :return: 预测结果
    """

    # 如果股票代码列存在，才进行以下操作
    if 'Symbol' in X_test.columns:
        # 分离股票代码列
        stock_code_test = X_test['Symbol']

        # 标准化训练集特征（不包括股票代码列）
        sca_X_test = StandardScaler()
        X_test_scaled = sca_X_test.fit_transform(X_test.drop(columns=['Symbol']))

        # 将股票代码列重新添加回去
        X_test_scaled = np.hstack((stock_code_test.values.reshape(-1, 1), X_test_scaled))
    else:
        # 标准化训练集特征（包括股票代码列）
        sca_X_test = StandardScaler()
        X_test_scaled = sca_X_test.fit_transform(X_test)

    # 使用模型进行预测
    y_predict = model.predict(X_test_scaled)

    # 评估模型
    # 计算均方误差
    mse = mean_squared_error(y_test, y_predict)
    print(f'Mean Squared Error: {mse}')

    # 显示模型的系数
    print(f'Coefficients: {model.coef_}')

    # 评估准确性
    accuracy = model.score(X_test_scaled, y_test)
    print("Accuracy:\n", accuracy)

def predict(model, X_lately, scaler):
    """
    预测未来数据
    :param model: 训练好的模型
    :param X_lately: 预测数据的特征
    :param X_train_scaled: 训练集特征
    :return: 预测结果
    """
    # 如果股票代码列存在，才进行以下操作
    if 'Symbol' in X_lately.columns:
        # 分离股票代码列
        stock_code_train = X_lately['Symbol']

        # 标准化训练集特征（不包括股票代码列）
        X_lately = scaler.transform(X_lately.drop(columns=['Symbol']))

        # 将股票代码列重新添加回去
        X_lately = np.hstack((stock_code_train.values.reshape(-1, 1), X_lately))
    else:
        # 标准化训练集特征（包括股票代码列）
        X_lately = scaler.fit_transform(scaler)

    # 使用模型进行预测
    forecast_set = model.predict(X_lately)

    return forecast_set

def plot_test_predict_graph(y_test, y_pred, date_col):
    """
    绘制预测结果图
    :param y_test: 测试集标签
    :param y_pred: 预测结果
    :param date_col: 日期列
    :return: None
    """
    # 取得测试集的日期
    test_date_time = date_col[-len(y_test):]
    print(test_date_time)

    # 设置图形的大小
    plt.figure(figsize=(10, 6))

    # 绘制实际值与预测值的曲线图
    plt.plot(test_date_time, y_test, label='Actual Prices')  # 绘制真实值
    plt.plot(test_date_time, y_pred, label='Predicted Prices')  # 绘制预测值

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

def plot_predict_graph(stock_data, forecast_set):
    # 修改matplotlib样式
    style.use('ggplot')  # ggplot是背景网格
    one_day = 86400  # 一天等于86400秒

    # 在data中新建列Forecast，用于存放预测结果的数据
    stock_data['Forecast'] = np.nan

    # 取data最后一行的时间索引
    last_date = stock_data.iloc[-1].name
    # 转化为时间戳
    last_unix = last_date.timestamp()
    # time.mktime(time.strptime(last_date,"%Y/%m/%d"))
    # 加一天的时间，跳转到下一天
    next_unix = last_unix + one_day
    # 遍历预测结果，用它向data中追加行

    # 这些行除了Forecast字段，其他都设为np.nan
    for i in forecast_set:
        # 此命令是时间戳转成时间，得到的类型也是datetime类型 ，类似于“2017-11-08 08:00:00”
        next_date = datetime.datetime.fromtimestamp(next_unix)
        next_unix += one_day
        # 这里要用定位的话应该是字符串，所以这里的格式还应该经过测试之后再索引
        # strftime()函数用来截取时间
        # [np.nan for _ in range(len(data.columns)-1)]生成不包含Forecast字段的列表
        stock_data.loc[next_date.strftime("%Y/%m/%d")] = [np.nan for _ in range(len(stock_data.columns) - 1)] + [i]

    stock_data['Close'].plot()
    stock_data['Forecast'].plot()
    plt.legend(loc=1)
    plt.xlabel('Date')
    plt.ylabel('Price')
    plt.show()

# 主程序
if __name__ == "__main__":

    # JSON文件路径
    json_file_path = './stock_data_code/stock_data/company_history_data/sh_a_stocks.json'

    # 加载股票代码
    stock_symbols = load_stock_symbols(json_file_path)

    # 定义日期范围
    start_date = "20190101"
    end_date = "20231231"

    # 获取股票数据
    all_stock_data = get_multiple_daily_stock_data(stock_symbols, start_date, end_date)
    print(all_stock_data.columns)

    # 数据预处理,得到原始特征和标签、需要预测的特征、原始特征列名，日期列, 合并后的数据集（包含标签列）
    X, y, X_lately, all_stock_data = preprocess_data(all_stock_data,5)
    print("标签数据:\n", y)

    # 选择最优特征，得到原始最优特征数据和标准化后的最优特征数据(暂不使用)
    selected_features = select_best_features(X, y, 'chi2',5)
    print("选择的特征列名:\n", selected_features)

    """
    # 使得X只包含最优特征
    X = X[selected_features]
    print(X)
    # 训练模型
    model, X_test, y_test = train(X, y)
    print("得到测试集特征、测试集标签:\n", X_test, y_test)

    # 预测并绘制预测结果'
    test(model, X_test, y_test)
    """

    # 得到你们需要预测的股票代码,根据股票代码列表随机获得一个股票代码
    symbol = random.choice(stock_symbols)
    print("随机选择的股票代码为：\n", symbol)

    # 获取随机选择的股票数据
    stock_data = get_single_daily_stock_data("603005", start_date, end_date)
    print("随机选择的股票数据为：\n", stock_data)

    # 将日期列设置为索引
    stock_data = stock_data.set_index('Date')
    # print("将日期列设置为索引后的数据为:\n", stock_data)

    # 获取需要预测的天数
    forecast_out = int(math.ceil(0.01 * len(stock_data)))
    print("需要预测的天数：\n", forecast_out)

    # 得到'HL_PCT', 'PCT_change'
    stock_data['HL_PCT'] = (stock_data['High'] - stock_data['Low']) / stock_data['Low'] * 100.0
    stock_data['PCT_change'] = (stock_data['Close'] - stock_data['Open']) / stock_data['Open'] * 100.0
    print("添加了'HL_PCT', 'PCT_change'后的股票数据为:\n", stock_data)

    # 数据预处理
    X_cur, y, X_lately, stock_data = preprocess_data(stock_data, forecast_out)

    # 训练模型
    model, X_test, y_test,scaler = train(X_cur,X_lately, y)
    print("得到测试集特征、测试集标签:\n", X_test, y_test)

    # 测试模型
    test(model, X_test, y_test)

    # 预测结果
    forecast_set = predict(model, X_lately,scaler)
    print("预测结果为:\n", forecast_set)

    # 绘制预测结果
    plot_predict_graph(stock_data, forecast_set)
"""
    # 修改matplotlib样式
    style.use('ggplot')  # ggplot是背景网格
    one_day = 86400  # 一天等于86400秒

    # 在data中新建列Forecast，用于存放预测结果的数据
    stock_data['Forecast'] = np.nan

    # 取data最后一行的时间索引
    last_date = stock_data.iloc[-1].name
    # 转化为时间戳
    last_unix = last_date.timestamp()
    # time.mktime(time.strptime(last_date,"%Y/%m/%d"))
    # 加一天的时间，跳转到下一天
    next_unix = last_unix + one_day
    # 遍历预测结果，用它向data中追加行

    # 这些行除了Forecast字段，其他都设为np.nan
    for i in forecast_set:
        # 此命令是时间戳转成时间，得到的类型也是datetime类型 ，类似于“2017-11-08 08:00:00”
        next_date = datetime.datetime.fromtimestamp(next_unix)
        next_unix += one_day
        # 这里要用定位的话应该是字符串，所以这里的格式还应该经过测试之后再索引
        # strftime()函数用来截取时间
        # [np.nan for _ in range(len(data.columns)-1)]生成不包含Forecast字段的列表
        stock_data.loc[next_date.strftime("%Y/%m/%d")] = [np.nan for _ in range(len(stock_data.columns) - 1)] + [i]

    stock_data['Close'].plot()
    stock_data['Forecast'].plot()
    plt.legend(loc=1)
    plt.xlabel('Date')
    plt.ylabel('Price')
    plt.show()
"""
"""
    # 训练模型，得到训练好的模型、测试集特征、测试集标签
    model, X_test, y_test = train(X, y)
    print(X_test, y_test)

    # 预测并得到预测的真实值
    y_pred = predict(model, X_test, y_test)
    # print(y_pred)

    # 标准化
    # X_lately = StandardScaler().fit_transform(X_lately)
    # forecast_set = model.predict(X_lately)
    # print(forecast_set)

    # 绘制实际值与预测值的曲线图
    # plot_predict_graph(all_stock_data, forecast_set, date_col)

    # 绘制实际值与预测值的曲线图
    plot_test_predict_graph(y_test, y_pred, date_col)
"""




