import json
import math
import numpy as np
import pandas as pd
from predict_stock_data_LR import get_single_daily_stock_data
from predict_stock_data_LR import load_stock_symbols
from predict_stock_data_LR import preprocess_data
from predict_stock_data_LR import train
from predict_stock_data_LR import test
from predict_stock_data_LR import predict
from predict_stock_data_LR import plot_predict_graph

# def save_data_to_json(data, filename):


def predict_daily_stock_data(start_date, end_date, symbol):

    # 选择最优特征
    selected_features = ['Open', 'Close', 'High', 'Low', 'Turnover']

    # 获取随机选择的股票数据
    stock_data = get_single_daily_stock_data(symbol, start_date, end_date)
    print("随机选择的股票数据为：\n", stock_data)

    # 将日期列设置为索引
    stock_data = stock_data.set_index('Date')
    print("将日期列设置为索引后的数据为:\n", stock_data)

    # 获取需要预测的天数
    forecast_out = int(math.ceil(0.01 * len(stock_data)))
    print("需要预测的天数：\n", forecast_out)

    # 得到'HL_PCT', 'PCT_change'
    stock_data[('H'
                'L_PCT')] = (stock_data['High'] - stock_data['Low']) / stock_data['Low'] * 100.0
    stock_data['PCT_change'] = (stock_data['Close'] - stock_data['Open']) / stock_data['Open'] * 100.0
    print("添加了'HL_PCT', 'PCT_change'后的股票数据为:\n", stock_data.columns)

    # 使得stock_data只包含最优特征selected_features列和'Close'列
    # temp = pd.DataFrame(stock_data, columns=selected_features)
    # if 'Close' not in temp.columns:
    #     stock_data['Close'] = temp['Close']
    # print("只包含最优特征列和'Close'列的股票数据列名为:\n", stock_data)

    # 数据预处理
    X_cur, y, X_lately, stock_data = preprocess_data(stock_data, forecast_out)
    print("预处理后的数据的特征列名为:\n", X_cur.columns)

    # 训练模型
    model, X_test, y_test, scaler = train(X_cur, y)
    print("测试集特征:\n", X_test)
    print("测试集标签:\n", y_test)

    # 测试模型
    test(model, X_test, y_test)

    # 预测结果
    forecast_set = predict(model, X_lately, scaler)
    print("预测结果为:\n", forecast_set)

    # 绘制预测结果
    plot_predict_graph(stock_data, forecast_set)

def predict_ten_times_daily_stock_data(symbol):
    pass

if __name__ == '__main__':
    # 从控制台获取参数
    # start_date = input("请输入开始日期（格式：YYYY-MM-DD）：")
    # end_date = input("请输入结束日期（格式：YYYY-MM-DD）：")
    # symbol = input("请输入股票代码：")

    json_path1 = '../data/stock_data/sh_a_stocks.json'
    json_path2 = '../data/stock_data/sz_a_stocks.json'

    # 读取这两个json文件，获取股票代码列表
    stock_symbols1 = load_stock_symbols(json_path1)

    # 取前250只股票代码
    stock_symbols1 = stock_symbols1[:250]

    # 读取这两个json文件，获取股票代码列表
    stock_symbols2 = load_stock_symbols(json_path2)

    # 取前250只股票代码
    stock_symbols2 = stock_symbols2[:250]

    # 合并两个股票代码列表
    stock_symbols = stock_symbols1 + stock_symbols2
    print("股票代码列表为：\n", stock_symbols)

    # 遍历这些股票代码，调用预测函数（先取5个）
    for symbol in stock_symbols:
        # 如果出现异常，则跳过
        try:
            predict_daily_stock_data('20200101', '20231231', symbol)
        except:
            print("出现异常，股票代码为：", symbol)
            continue

    # 调用预测函数
    # predict_daily_stock_data(start_date, end_date, symbol)
    #
    # predict_ten_times_daily_stock_data(symbol)



