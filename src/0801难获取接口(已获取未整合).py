import os
import json
import numpy as np
import pandas as pd
import akshare as ak
import inspect
import keyboard
import time
from datetime import date, datetime, timedelta
from basic_func import DateEncoder
from basic_func import save_to_json
from basic_func import save_to_json_v2
from basic_func import load_json
from basic_func import load_json_df
from basic_func import get_yesterday
from basic_func import processing_date
from basic_func import find_latest_file
from basic_func import find_latest_file_v2
from basic_func import stock_traversal_module
from basic_func import get_matching_h_stocks
from basic_func import create_dict
from basic_func import is_holiday
from basic_func import is_weekend

from datetime import datetime, timedelta
import os
import pandas as pd
import inspect


def stock_traversal_module_check(func, basic_name, stock_dict, flag, args, base_path='./stock_data/company_data',
                                 report_date=get_yesterday(), get_full_file=False, individual_file=True,
                                 date_func=None, quarter_list=None, use_quarter=False):
    """
    适用于需要动态生成 date 或 quarter 参数的股票数据遍历函数
    """
    debug = True
    # 加载中断点记录
    interrupt_file = os.path.join(base_path, f'{basic_name}_interrupt_{report_date}.json')
    interrupt_data = load_json(interrupt_file)
    if not isinstance(interrupt_data, dict):
        interrupt_data = {}
    processed_stocks = set(interrupt_data.get('processed_stocks', []))

    # 错误报告的读取
    error_file = os.path.join(base_path, f"{basic_name}_error_reports_{report_date}.json")
    error_reports = load_json(error_file)
    if not isinstance(error_reports, list):
        error_reports = []

    # 已处理数据的读取
    if get_full_file:
        data_file = os.path.join(base_path, f"{basic_name}_full_data_{report_date}.json")
        processed_data = load_json(data_file)
        if not isinstance(processed_data, list):
            processed_data = []

    total_stocks = len(stock_dict)
    for i, stock in enumerate(stock_dict):
        stock_code = stock['代码']
        stock_name = stock['名称']

        # 跳过已处理的股票
        if stock_code in processed_stocks:
            continue

        try:
            company_name = stock["名称"].strip()
            if company_name.startswith("ST") or company_name.startswith("*ST"):
                continue
            company_name_safe = company_name.replace("*", "")
            market = "深A股" if flag else "沪A股"
            targeted_filepath = os.path.join(base_path, market, company_name_safe) if individual_file else os.path.join(
                base_path, market)
            os.makedirs(targeted_filepath, exist_ok=True)

            # 根据不同情况生成参数列表
            if use_quarter:
                # 使用给定的季度列表遍历
                for quarter in quarter_list:
                    current_args = args.copy()
                    current_args["stock"] = stock_code
                    current_args["quarter"] = quarter
                    interface_df = func(**current_args)
                    filepath = os.path.join(targeted_filepath, f"{company_name_safe}_{basic_name}_{quarter}.json")
                    save_to_json_v2(interface_df, filepath)

            else:
                # 使用 date_func 获取日期列表并遍历
                date_list = date_func(symbol=stock_code) if date_func else args.get("date_list", [])
                for date in date_list:
                    for flag_option in ["买入", "卖出"]:
                        current_args = args.copy()
                        current_args["symbol"] = stock_code
                        current_args["date"] = date
                        current_args["flag"] = flag_option
                        interface_df = func(**current_args)
                        filepath = os.path.join(targeted_filepath,
                                                f"{company_name_safe}_{basic_name}_{date}_{flag_option}.json")
                        save_to_json_v2(interface_df, filepath)

            processed_stocks.add(stock_code)

            # 定期保存中间结果和中断点
            if (i + 1) % 10 == 0 or i == total_stocks - 1:
                if get_full_file:
                    save_to_json(processed_data, data_file)
                save_to_json({"processed_stocks": list(processed_stocks)}, interrupt_file)
                save_to_json(error_reports, error_file)
                print(f"Progress: {i + 1}/{total_stocks} stocks processed.")

        except Exception as e:
            print(f"Error processing stock {stock_code}: {e}")
            error_reports.append({"stock_name": stock_name, "stock_code": stock_code, "flag": flag, "Error": str(e)})
            if (i + 1) % 10 == 0 or i == total_stocks - 1:
                if get_full_file:
                    save_to_json(processed_data, data_file)
                save_to_json({"processed_stocks": list(processed_stocks)}, interrupt_file)
                save_to_json(error_reports, error_file)
                print(f"Progress: {i + 1}/{total_stocks} stocks processed.")
            continue

        # 保存最终结果
        if get_full_file:
            save_to_json(processed_data, data_file)
        save_to_json({"processed_stocks": list(processed_stocks)}, interrupt_file)
        save_to_json(error_reports, error_file)

# 假设存在一个函数，用于获取某只股票的龙虎榜日期
def get_lhb_date_list(symbol):
    date_list = ak.stock_lhb_stock_detail_date_em(symbol=symbol)["交易日"]
    return [pd.to_datetime(date).strftime('%Y%m%d') for date in date_list]


# 定义股票字典
stock_dict = [{"代码": "000001", "名称": "平安银行"}, {"代码": "600000", "名称": "浦发银行"}]

# 调用 stock_traversal_module_check 处理 "stock_lhb_stock_detail_em" 接口
stock_traversal_module_check(
    func=ak.stock_lhb_stock_detail_em,
    basic_name="stock_lhb_stock_detail_em",
    stock_dict=stock_dict,
    flag=1,
    args={},
    base_path='./stock_data/company_data',
    date_func=get_lhb_date_list,
    use_quarter=False
)

# 定义季度列表
quarter_list = ["20231", "20232", "20233", "20234"]

# 调用 stock_traversal_module_check 处理 "stock_institute_hold_detail" 接口
stock_traversal_module_check(
    func=ak.stock_institute_hold_detail,
    basic_name="stock_institute_hold_detail",
    stock_dict=stock_dict,
    flag=1,
    args={},
    base_path='./stock_data/company_data',
    quarter_list=quarter_list,
    use_quarter=True
)
