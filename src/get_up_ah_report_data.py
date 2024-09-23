import os
import json
import numpy as np
import pandas as pd
import akshare as ak
from datetime import datetime, timedelta
from basic_func import save_to_json
from basic_func import save_to_json_v2
from basic_func import load_json
from basic_func import load_json_df
from basic_func import load_json_df_all_scalar
from basic_func import get_yesterday
from basic_func import find_latest_file_v2
from get_daily_reports import get_daily_report
from get_weekly_report_and_daily_up2 import check_daily_up_interface


def get_intersected_stocks(report_date, base_path="./stock_data"):

    company_data_filepath = os.path.join(base_path, "company_data")
    weekly_report_filepath = os.path.join(company_data_filepath, "weekly_report")
    target_prefix_sz = "daily_up_report_up_stock_sz"
    target_prefix_sh = "daily_up_report_up_stock_sh"

    matching_h_stocks_filepath = os.path.join(company_data_filepath, "matching_h_stocks.json")

    sz_up_stock_filepath = find_latest_file_v2(weekly_report_filepath, target_prefix_sz, report_date, report_date)
    sh_up_stock_filepath = find_latest_file_v2(weekly_report_filepath, target_prefix_sh, report_date, report_date)
    output_path_sz = os.path.join(weekly_report_filepath, f"intersected_stocks_sz_{report_date}.json")
    output_path_sh = os.path.join(weekly_report_filepath, f"intersected_stocks_sh_{report_date}.json")

    with open(matching_h_stocks_filepath, 'r', encoding='utf-8') as file:
        matching_h_stocks = json.load(file)
    with open(sz_up_stock_filepath, 'r', encoding='utf-8') as file:
        daily_up_report_sz = json.load(file)
    with open(sh_up_stock_filepath, 'r', encoding='utf-8') as file:
        daily_up_report_sh = json.load(file)

    a_to_h_map = {item['A股代码']: item['H股代码'] for item in matching_h_stocks}
    # 找到daily_up_report中代码在a_to_h_map中的数据，并包括对应的H股代码
    intersection_data_sz = [
        {
            "代码": item['代码'],
            "名称": item['名称'],
            "H股代码": a_to_h_map[item['代码']]
        }
        for item in daily_up_report_sz if item['代码'] in a_to_h_map
    ]
    intersection_data_sh = [
        {
            "代码": item['代码'],
            "名称": item['名称'],
            "H股代码": a_to_h_map[item['代码']]
        }
        for item in daily_up_report_sh if item['代码'] in a_to_h_map
    ]

    # 保存交集数据到新文件
    with open(output_path_sz, 'w', encoding='utf-8') as file:
        json.dump(intersection_data_sz, file, ensure_ascii=False, indent=4)
    with open(output_path_sh, 'w', encoding='utf-8') as file:
        json.dump(intersection_data_sh, file, ensure_ascii=False, indent=4)

def merge_up_ah_data(stock_dict, processed_stocks, flag, report_date, interrupt_file, base_path="./stock_data/company_data", existing_data=True):
    """
    实现需求五.3
    输出每日A-H股中上涨的A股以及对应的H股：公司名称、A股简称、股票代码、总股本、今日开盘股价、今日收盘股价（也是今日收盘后的实时数据）、
    今日开盘总市值、今日收盘总市值（公司的，至少港股没有）、今日涨跌、今日涨跌幅（stock_zh_ah_spot后的公司实时数据）、今日收盘发行市盈率
    :param stock_dict: 已生成的的深A或沪A股字典
    :param base_path: 每日报表生成的基本路径
    :param processed_stocks: 中断处理使用，记录已处理的公司
    :param flag: 1表示深A股字典，0表示沪A股字典；内部需要使用不同接口
    :param report_date: 指定的日期
    :return: 无返回值，直接写入文件并存储
    """
    frequency = 300
    if not existing_data:
        stock_sh_a_spot_em_df = ak.stock_sh_a_spot_em()
        stock_sz_a_spot_em_df = ak.stock_sz_a_spot_em()

    merge_up_ah_data_file = os.path.join(base_path, f"merge_up_ah_data_{report_date}.json")
    error_reports_file = os.path.join(base_path, f"merge_up_ah_data_error_reports_{report_date}.json")

    merge_up_ah_data = load_json(merge_up_ah_data_file)
    error_reports = load_json(error_reports_file)

    if not isinstance(merge_up_ah_data, list):
        merge_up_ah_data = []

    if not isinstance(error_reports, list):
        error_reports = []

    daily_report_interrupt_file = os.path.join(base_path, f'daily_reports_interrupt_{report_date}.json')
    daily_report_interrupt_data = load_json(daily_report_interrupt_file)
    # Ensure interrupt_data is a dictionary
    if not isinstance(daily_report_interrupt_data, dict):
        daily_report_interrupt_data = {}
    daily_report_processed_stocks = set(daily_report_interrupt_data.get('processed_stocks', []))

    total_stocks = len(stock_dict)
    for i, stock in enumerate(stock_dict):
        stock_code = stock['代码']
        stock_name = stock['名称']
        h_stock_code = stock['H股代码']

        # 跳过已处理的股票
        if stock_code in processed_stocks:
            # print(f"公司 {stock_name} 代码 {stock_code}已处理，跳过 ")
            continue

        try:
            # 先计算存储路径；将内容装在入对应公司文件夹中
            company_name = stock["名称"].strip()  # 去除名称两端的空格
            # 如果公司名称以 "ST" 开头，则跳过当前循环
            if company_name.startswith("ST") or company_name.startswith("*ST"):
                continue
            # 替换非法字符
            company_name_safe = company_name.replace("*", "")  # 替换 * 字符

            report_date_str = report_date.replace("-", "").replace(":", "").replace(" ", "")
            if flag:
                market = "深A股"
                # region = "sz"
            else:
                market = "沪A股"
                # region = "sh"

            # 写入的文件路径
            acquired_data_file = os.path.join(base_path, market, company_name_safe,
                                             f"{company_name_safe}_merge_up_ah_data.json")
            os.makedirs(os.path.dirname(acquired_data_file), exist_ok=True)
            company_file = os.path.join(base_path, market, company_name_safe)

            daily_report_file = os.path.join(base_path, market, company_name_safe,
                                             f"{company_name_safe}_daily_report_{report_date_str}.json")
            daily_report_df = load_json_df_all_scalar(daily_report_file)
            if (not existing_data) and daily_report_df.empty:
                get_daily_report([{"代码": f"{stock_code}", "名称": f"{stock_name}"}], base_path, daily_report_processed_stocks,
                                 flag, report_date, stock_sh_a_spot_em_df, stock_sz_a_spot_em_df, daily_report_interrupt_file)
            elif daily_report_df.empty:
                raise ValueError("existing_data=True，而又没获取到已有的对应日期每日报表")
            daily_report_row = daily_report_df.iloc[0]

            basic_prefix = "stock_sh_hist"
            stock_hk_hist_path = os.path.join(base_path, "H_stock", company_name_safe,
                                           f"{company_name_safe}_stock_hk_hist_{report_date_str}.json")
            stock_hk_hist_df = load_json_df(stock_hk_hist_path)
            if stock_hk_hist_df.empty:
                stock_hk_hist_df = ak.stock_hk_hist(symbol=h_stock_code, period="daily", start_date=report_date,
                                                   end_date=report_date, adjust="qfq")
                if stock_hk_hist_df.empty:
                    print(f"无法获取公司 {stock_name} 代码 {stock_code} 的历史行情数据，对应接口：ak.stock_zh_a_hist")
                    error_reports.append({"stock_name": stock_name, "stock_code": stock_code, "flag": flag})
                    continue
                format_date_form(stock_hk_hist_df)
                save_to_json_v2(stock_hk_hist_df, stock_hk_hist_path)

            h_opening_price_today = stock_hk_hist_df.at[0, '开盘']
            h_closing_price_today = stock_hk_hist_df.at[0, '收盘']
            h_price_change_today = stock_hk_hist_df.at[0, '涨跌额']
            h_price_change_percentage_today = stock_hk_hist_df.at[0, '涨跌幅']

            hk_valuation_baidu_zsz_filepath = os.path.join(base_path, "H_stock", company_name_safe,
                                           f"{company_name_safe}_hk_valuation_baidu_zsz_1y_{report_date_str}.json")
            df1 = load_json_df(hk_valuation_baidu_zsz_filepath)
            if df1.empty:
                df1 = ak.stock_hk_valuation_baidu(symbol=h_stock_code, indicator="总市值", period="近一年")
                format_date_form(df1)
                save_to_json_v2(df1, hk_valuation_baidu_zsz_filepath)
            target_date = datetime.strptime(report_date_str, "%Y%m%d").strftime("%Y-%m-%d")

            # 直接匹配目标日期
            target_date_value = df1[df1['date'] == target_date]['value']
            if target_date_value.empty:
                raise ValueError(f"没有找到目标日期 {target_date} 的数据")
            h_market_cap_closing = target_date_value.values[0]

            # 获取前一天的日期并匹配
            previous_date = (datetime.strptime(target_date, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
            previous_date_value = df1[df1['date'] == previous_date]['value']
            if previous_date_value.empty:
                raise ValueError(f"没有找到前一天 {previous_date} 的数据")
            h_market_cap_opening = previous_date_value.values[0]

            hk_valuation_baidu_sylTTM_filepath = os.path.join(base_path, "H_stock", company_name_safe,
                                           f"{company_name_safe}_hk_valuation_baidu_sylTTM_1y_{report_date_str}.json")
            df2 = load_json_df(hk_valuation_baidu_sylTTM_filepath)
            if df2.empty:
                df2 = ak.stock_hk_valuation_baidu(symbol=h_stock_code, indicator="市盈率(TTM)", period="近一年")
                format_date_form(df2)
                save_to_json_v2(df2, hk_valuation_baidu_sylTTM_filepath)
            target_date_value = df2[df2['date'] == target_date]['value']
            if target_date_value.empty:
                raise ValueError(f"没有找到目标日期 {target_date} 的数据")
            h_pe_ratio_today = target_date_value.values[0]

            acquired_data = {
                "公司名称": daily_report_row["公司名称"],
                "总股本": daily_report_row["总股本"],  # 这是A股和H股共有的属性
                "A股简称": stock_name,
                "A股股票代码": stock_code,
                "A股今日开盘股价": daily_report_row["今日开盘股价"],
                "A股今日收盘股价": daily_report_row["今日收盘股价"],
                "A股今日开盘总市值": daily_report_row["今日开盘总市值"],
                "A股今日收盘总市值": daily_report_row["今日收盘总市值"],
                "A股今日涨跌": daily_report_row["今日涨跌"],
                "A股今日涨跌幅": daily_report_row["今日涨跌幅"],
                "A股今日收盘市盈率": daily_report_row["今日收盘发行市盈率"],
                "H股股票代码": h_stock_code,
                "H股今日开盘股价": h_opening_price_today,
                "H股今日收盘股价": h_closing_price_today,
                "H股今日开盘总市值": h_market_cap_opening,
                "H股今日收盘总市值": h_market_cap_closing,
                "H股今日涨跌": h_price_change_today,
                "H股今日涨跌幅": h_price_change_percentage_today,
                "H股今日收盘市盈率": h_pe_ratio_today
            }

            merge_up_ah_data.append(acquired_data)

            # 将每日报表保存为JSON文件
            save_to_json(acquired_data, acquired_data_file)

            # 记录已处理的股票
            processed_stocks.add(stock_code)

            # 定期保存中间结果和中断点
            if (i + 1) % 10 == 0 or i == total_stocks - 1:
                save_to_json(merge_up_ah_data, merge_up_ah_data_file)
                save_to_json({"processed_stocks": list(processed_stocks)}, interrupt_file)
                save_to_json(error_reports, error_reports_file)
                if (i + 1) % 300 == 0:
                    print(f"Progress: {i + 1}/{total_stocks} stocks processed.")

        except Exception as e:
            print(f"Error processing stock {stock_code}: {e}")
            error_reports.append({"stock_name": stock_name, "stock_code": stock_code, "flag": flag})
            if (i + 1) % 10 == 0 or i == total_stocks - 1:
                save_to_json(merge_up_ah_data, merge_up_ah_data_file)
                save_to_json({"processed_stocks": list(processed_stocks)}, interrupt_file)
                save_to_json(error_reports, error_reports_file)
                if (i + 1) % 300 == 0:
                    print(f"Progress: {i + 1}/{total_stocks} stocks processed.")
            continue

        # 保存最终结果
        save_to_json(merge_up_ah_data, merge_up_ah_data_file)
        save_to_json({"processed_stocks": list(processed_stocks)}, interrupt_file)
        save_to_json(error_reports, error_reports_file)


def get_merge_up_ah_data(base_path='./stock_data', report_date=get_yesterday()):
    """
    A-H上涨A股总输出报表
    :param base_path: 基本路径，默认为'./stock_data'。
    :param report_date 指定每日报告的日期，YYYYMMDD的str，默认是昨天
    :return:
    """
    print("Now into function get_merge_up_ah_data \n")

    # # 指定日期
    # report_date = get_yesterday()
    company_data_filepath = os.path.join(base_path, "company_data")
    weekly_report_filepath = os.path.join(company_data_filepath, "weekly_report")
    sh_up_ah_dict = os.path.join(weekly_report_filepath, f"intersected_stocks_sz_{report_date}.json")
    sz_up_ah_dict = os.path.join(weekly_report_filepath, f"intersected_stocks_sh_{report_date}.json")

    # 魔改了的文件路径 company_data_filepath
    company_data_filepath = "E:/Project_storage/stock_data/company_data"

    sh_a_stocks = load_json(sh_up_ah_dict)
    sz_a_stocks = load_json(sz_up_ah_dict)

    # 加载中断点记录
    interrupt_file = os.path.join(company_data_filepath, f'merge_up_ah_data_interrupt_{report_date}.json')
    interrupt_data = load_json(interrupt_file)
    # Ensure interrupt_data is a dictionary
    if not isinstance(interrupt_data, dict):
        interrupt_data = {}
    processed_stocks = set(interrupt_data.get('processed_stocks', []))

    merge_up_ah_data(sh_a_stocks, processed_stocks, 0, report_date, interrupt_file, company_data_filepath, False)
    merge_up_ah_data(sz_a_stocks, processed_stocks, 1, report_date, interrupt_file, company_data_filepath, False)
    print("Successful executing function get_merge_up_ah_data \n")

def format_date_form(df):
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].apply(
                lambda x: x.isoformat() if pd.notnull(x) else None)
        elif pd.api.types.is_object_dtype(df[col]):
            df[col] = df[col].astype(str)


def get_up_ah_report_data(report_date, base_path="./stock_data"):
    print("Now executing function: get_up_ah_report_data")
    company_base_path = os.path.join(base_path, "company_data")
    # report_date = "20240809"

    check_daily_up_interface(date=report_date, base_path=base_path, creating_new_dict=True)
    get_intersected_stocks(report_date=report_date, base_path=base_path)
    get_merge_up_ah_data(report_date=report_date, base_path=base_path)  # 魔改了的文件路径 company_data_filepath，可以直接注释对应行
    print("Successfully executing function get_up_ah_report_data")


if __name__ == "__main__":

    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_file_path = os.path.join(project_root, 'conf', 'config.json')
    # 读取配置文件
    with open(config_file_path, 'r') as f:
        config = json.load(f)
    base_path = config['base_path']

    # is_local = True
    # if is_local:
    #     base_path = './stock_data'
    # else:
    #     current_dir = os.path.dirname(os.path.abspath(__file__))
    #     parent_dir = os.path.dirname(current_dir)
    #     base_path = os.path.join(parent_dir, 'data', 'stock_data')
    #     os.makedirs(os.path.join(parent_dir, 'data', 'stock_data'), exist_ok=True)
    #     os.makedirs(os.path.join(parent_dir, 'data', 'stock_data/company_data'), exist_ok=True)
    #     os.makedirs(os.path.join(parent_dir, 'data', 'stock_data/company_data/深A股'), exist_ok=True)
    #     os.makedirs(os.path.join(parent_dir, 'data', 'stock_data/company_data/沪A股'), exist_ok=True)
    #     os.makedirs(os.path.join(parent_dir, 'data', 'stock_data/company_data/weekly_report'), exist_ok=True)

    company_path = os.path.join(base_path, "company_data")
    report_date = datetime.now().strftime("%Y%m%d")

    check_daily_up_interface(date=report_date, base_path=base_path, creating_new_dict=True)
    get_intersected_stocks(report_date=report_date, base_path=base_path)
    get_merge_up_ah_data(report_date=report_date, base_path=base_path)  # 魔改了的文件路径 company_data_filepath，可以直接注释对应行
