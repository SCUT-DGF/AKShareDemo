import os
import json
import numpy as np
import pandas as pd
import akshare as ak
import inspect
# import keyboard
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


# 240801版本
# 外用接口：
# report_date = "20240731"
# check_daily_up_interface(report_date, creating_new_dict=False)
# week_report_date = "20240726"
# get_weekly_reports(week_report_date, report_date)
# get_up_stock_interface(report_date) 已删除

def get_week_range(date_str, check_time=False):
    """
    输入一个YYYYMMDD日期，输出以周一至周日为一周的，该日期所属周的周一与周五的日期
    :param date_str: 输入的日期字符串
    :param check_time: 若设为True，若当前时间未到所属周的周五下午3点，会返回错误信息
    :return: 该日期所属周的周一与周五的日期字符串
    """
    # 将字符串转换为日期对象
    date = datetime.strptime(date_str, '%Y%m%d')

    # 计算这个日期对应的星期一的日期
    start_of_week = date - timedelta(days=date.weekday())

    # 计算这个日期对应的星期五的日期
    end_of_week = start_of_week + timedelta(days=4)

    # 返回起始日期和结束日期，格式为 %Y%m%d
    start_of_week_str = start_of_week.strftime('%Y%m%d')
    end_of_week_str = end_of_week.strftime('%Y%m%d')

    if check_time:
        # 计算这个星期五下午3点的时间
        friday_3pm = end_of_week + timedelta(hours=15)

        # 获取当前时间
        now = datetime.now()

        # 如果当前时间没有过星期五下午3点，则报错
        if now < friday_3pm:
            raise ValueError("当前时间还没有过该日期段的星期五下午3点")

    return start_of_week_str, end_of_week_str

def get_weekly_report(func, basic_name, stock_dict, flag, args, base_path='./stock_data/company_data',
                           report_date=get_yesterday(), get_full_file=True,  individual_file=True):
    """
    获取每日报表
    :param func: 调用的接口函数
    :param stock_dict: 已生成的的深A或沪A股字典
    :param basic_name: 接口的基本名称，用于给各文件命名
    :param stock_dict: 遍历的股票字典
    :param flag: 1表示深A股字典，0表示沪A股字典；内部需要使用不同接口
    :param args: 接口需要的股票代码外的参数
    :param base_path: 每日报表生成的基本路径
    :param report_date: 数据储存时文件的后缀id，默认为日期，为日期时可用find_latest_date函数
    :param individual_file: bool类型，数据文件存储公司文件夹还是深沪A股的大文件夹，默认为True即存入公司文件夹
    :return: 无返回值，直接写入文件并存储
    """
    frequency = 300
    debug = False
    # 加载中断点记录
    interrupt_file = os.path.join(base_path, f'{basic_name}_interrupt_{report_date}.json')
    interrupt_data = load_json(interrupt_file)
    if not isinstance(interrupt_data, dict):
        interrupt_data = {}
    processed_stocks = set(interrupt_data.get('processed_stocks', []))
    # 错误报告的读取
    error_file = os.path.join(base_path, f"{basic_name}_error_reports_{report_date}.json")
    error_reports = load_json(error_file)
    error_reports = []
    if not isinstance(error_reports, list):
        error_reports = []
    # 已处理数据的读取，用于获取完整内容
    report_path = os.path.join(base_path, 'weekly_report')
    os.makedirs(report_path, exist_ok=True)

    market_tag = "sz" if flag else "sh"
    data_file = os.path.join(os.path.join(report_path, f"all_weekly_report_{market_tag}_{report_date}.json"))
    up_3_days_file = os.path.join(report_path, f"{basic_name}_up_3_days_{market_tag}_{report_date}.json")
    up_4_days_file = os.path.join(report_path, f"{basic_name}_up_4_days_{market_tag}_{report_date}.json")
    up_5_days_file = os.path.join(report_path, f"{basic_name}_up_5_days_{market_tag}_{report_date}.json")

    processed_data = load_json_df(data_file)
    # if not isinstance(processed_data, list):
    #     processed_data = []
    if processed_data.empty:
        processed_data = pd.DataFrame(columns=["代码", "名称", "上涨天数"])

    df_up_3_days = load_json_df(up_3_days_file)
    if df_up_3_days.empty:
        df_up_3_days = pd.DataFrame(columns=["代码", "名称"])

    df_up_4_days = load_json_df(up_4_days_file)
    if df_up_4_days.empty:
        df_up_4_days = pd.DataFrame(columns=["代码", "名称"])

    df_up_5_days = load_json_df(up_5_days_file)
    if df_up_5_days.empty:
        df_up_5_days = pd.DataFrame(columns=["代码", "名称"])


    temp_args = inspect.signature(func).parameters
    is_symbol = "symbol" in temp_args


    # 遍历所有股票的字段
    total_stocks = len(stock_dict)
    for i, stock in enumerate(stock_dict):
        stock_code = stock['代码']
        stock_name = stock['名称']

        # # 为了方便调试，开启以下功能
        # if debug and keyboard.is_pressed('enter'):
        #     print(f"继续按回车键1秒跳过接口：{basic_name}")
        #     time.sleep(1)
        #     if keyboard.is_pressed('enter'):
        #         print(f"强制跳过接口：{basic_name}")
        #         return
        if debug and i >300:
            return


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
            market = "深A股" if flag else "沪A股"
            # # 写入的文件路径
            if individual_file:
                targeted_filepath = os.path.join(base_path, market, company_name_safe)
            else:
                targeted_filepath = os.path.join(base_path, market)  # 个股信息存储的路径
            os.makedirs(os.path.join(targeted_filepath, company_name_safe), exist_ok=True)
            filepath = os.path.join(targeted_filepath, f"{company_name_safe}_{basic_name}_{report_date}.json")

            # 通过args传递接口的其它参数
            if is_symbol:
                interface_df = func(symbol=stock_code, **args)
            else:
                interface_df = func(stock=stock_code, **args)

            if not isinstance(interface_df, pd.DataFrame):
                raise ValueError(f"{basic_name} does not return DataFrame ")
            if interface_df.empty:
                print(f"Fail to fetch {stock_name}  {stock_code} data，interface：{basic_name}")
                error_reports.append({"stock_name": stock_name, "stock_code": stock_code, "flag": flag,
                                      "Error": f"From {basic_name}m empty dataframe"})
                continue
            # 确保日期字段转换为字符串格式
            for col in interface_df.columns:
                if pd.api.types.is_datetime64_any_dtype(interface_df[col]):
                    interface_df[col] = interface_df[col].apply(lambda x: x.isoformat() if pd.notnull(x) else None)
                elif pd.api.types.is_object_dtype(interface_df[col]):
                    interface_df[col] = interface_df[col].astype(str)

            # 记录已处理的股票
            processed_stocks.add(stock_code)
            save_to_json_v2(interface_df, filepath)

            up_day = 0
            up_days = 0
            for index, row in interface_df.iterrows():
                if row['涨跌幅'] > 0:
                    up_day += 1
                    if up_day >= up_days:
                        up_days = up_day
                else:
                    up_day = 0
            # up_days = (interface_df["涨跌幅"] > 0).sum()
            if up_days >= 3:
                if up_days == 3:
                    df_up_3_days = pd.concat([df_up_3_days, pd.DataFrame([{"代码": stock_code, "名称": stock_name}])],
                                             ignore_index=True)
                    # df_up_3_days = df_up_3_days.append({"代码": stock_code, "名称": stock_name})
                elif up_days == 4:
                    df_up_4_days = pd.concat([df_up_4_days, pd.DataFrame([{"代码": stock_code, "名称": stock_name}])], ignore_index=True)
                    # df_up_4_days = df_up_4_days.append({"代码": stock_code, "名称": stock_name})
                elif up_days >= 5:
                    df_up_5_days = pd.concat([df_up_5_days, pd.DataFrame([{"代码": stock_code, "名称": stock_name}])], ignore_index=True)
                    # df_up_5_days = df_up_5_days.append({"代码": stock_code, "名称": stock_name})
            # processed_data.append({"代码": stock_code, "名称": stock_name, "上涨天数": up_days})
            processed_data = pd.concat([processed_data,pd.DataFrame([{"代码": stock_code, "名称": stock_name, "上涨天数": up_days}])], ignore_index=True)

            # 定期保存中间结果和中断点
            if (i + 1) % 10 == 0 or i == total_stocks - 1:
                if get_full_file:
                    save_to_json_v2(processed_data, data_file)
                save_to_json({"processed_stocks": list(processed_stocks)}, interrupt_file)
                save_to_json(error_reports, error_file)
                if (i + 1) % frequency == 0:
                    print(f"Progress: {i + 1}/{total_stocks} stocks processed.")

        except Exception as e:
            print(f"Error processing stock {stock_code}: {e}")
            error_reports.append({"stock_name": stock_name, "stock_code": stock_code, "flag": flag, "Error": str(e)})
            if (i + 1) % 10 == 0 or i == total_stocks - 1:
                if get_full_file:
                    save_to_json_v2(processed_data, data_file)
                save_to_json({"processed_stocks": list(processed_stocks)}, interrupt_file)
                save_to_json(error_reports, error_file)

                save_to_json_v2(df_up_3_days, up_3_days_file)
                save_to_json_v2(df_up_4_days, up_4_days_file)
                save_to_json_v2(df_up_5_days, up_5_days_file)
                if (i + 1) % frequency == 0:
                    print(f"Progress: {i + 1}/{total_stocks} stocks processed.")
            continue

        # 保存最终结果
        if get_full_file:
            save_to_json_v2(processed_data, data_file)
        save_to_json({"processed_stocks": list(processed_stocks)}, interrupt_file)
        save_to_json(error_reports, error_file)

        # 保存所有股票的报告
        # 保存分别上涨三、四、五天的报告
        if not df_up_3_days.empty:
            save_to_json_v2(df_up_3_days, os.path.join(report_path, f"{basic_name}_up_3_days_{report_date}.json"))
        if not df_up_4_days.empty:
            save_to_json_v2(df_up_4_days, os.path.join(report_path, f"{basic_name}_up_4_days_{report_date}.json"))
        if not df_up_5_days.empty:
            save_to_json_v2(df_up_5_days, os.path.join(report_path, f"{basic_name}_up_5_days_{report_date}.json"))


def check_daily_up(func, basic_name, stock_dict, flag, args, company_base_path='./stock_data/company_data',
                           report_date=get_yesterday(), get_full_file=True, individual_file=True):
    """
    获取每日报表
    :param func: 调用的接口函数
    :param stock_dict: 已生成的的深A或沪A股字典
    :param basic_name: 接口的基本名称，用于给各文件命名
    :param flag: 1表示深A股字典，0表示沪A股字典；内部需要使用不同接口
    :param args: 接口需要的股票代码外的参数
    :param company_base_path: 每日报表生成的基本路径
    :param report_date: 数据储存时文件的后缀id，默认为日期，为日期时可用find_latest_date函数
    :param individual_file: bool类型，数据文件存储公司文件夹还是深沪A股的大文件夹，默认为True即存入公司文件夹
    :return: 无返回值，直接写入文件并存储
    """
    frequency = 300
    debug = False
    # 加载中断点记录
    interrupt_file = os.path.join(company_base_path, f'{basic_name}_interrupt_{report_date}.json')
    interrupt_data = load_json(interrupt_file)
    if not isinstance(interrupt_data, dict):
        interrupt_data = {}
    processed_stocks = set(interrupt_data.get('processed_stocks', []))
    # 错误报告的读取
    error_file = os.path.join(company_base_path, f"{basic_name}_error_reports_{report_date}.json")
    error_reports = load_json(error_file)
    error_reports = []
    if not isinstance(error_reports, list):
        error_reports = []
    # 已处理数据的读取，用于获取完整内容
    report_path = os.path.join(company_base_path, 'weekly_report')
    os.makedirs(report_path, exist_ok=True)

    market_tag = "sz" if flag else "sh"
    data_file = os.path.join(os.path.join(report_path, f"all_up_range_report_{market_tag}_{report_date}.json"))
    up_stock_dict_file = os.path.join(report_path, f"{basic_name}_up_stock_{market_tag}_{report_date}.json")
    up_range13_file = os.path.join(report_path, f"{basic_name}_up_range13_{market_tag}_{report_date}.json")
    up_range35_file = os.path.join(report_path, f"{basic_name}_up_range35_{market_tag}_{report_date}.json")
    up_range57_file = os.path.join(report_path, f"{basic_name}_up_range57_{market_tag}_{report_date}.json")
    up_range710_file = os.path.join(report_path, f"{basic_name}_up_range710_{market_tag}_{report_date}.json")
    up_range10_file = os.path.join(report_path, f"{basic_name}_up_range10_{market_tag}_{report_date}.json")

    processed_data = load_json_df(data_file)
    # if not isinstance(processed_data, list):
    #     processed_data = []
    if processed_data.empty:
        processed_data = pd.DataFrame(columns=["代码", "名称", "涨幅"])

    df_up_stock_dict = load_json_df(up_stock_dict_file)
    if df_up_stock_dict.empty:
        df_up_stock_dict = pd.DataFrame(columns=["代码", "名称"])

    df_up_range13 = load_json_df(up_range13_file)
    if df_up_range13.empty:
        df_up_range13 = pd.DataFrame(columns=["代码", "名称"])

    df_up_range35 = load_json_df(up_range35_file)
    if df_up_range35.empty:
        df_up_range35 = pd.DataFrame(columns=["代码", "名称"])

    df_up_range57 = load_json_df(up_range57_file)
    if df_up_range57.empty:
        df_up_range57 = pd.DataFrame(columns=["代码", "名称"])

    df_up_range710 = load_json_df(up_range710_file)
    if df_up_range710.empty:
        df_up_range710 = pd.DataFrame(columns=["代码", "名称"])

    df_up_range10 = load_json_df(up_range10_file)
    if df_up_range10.empty:
        df_up_range10 = pd.DataFrame(columns=["代码", "名称"])

    temp_args = inspect.signature(func).parameters
    is_symbol = "symbol" in temp_args

    # 遍历所有股票的字段
    total_stocks = len(stock_dict)
    for i, stock in enumerate(stock_dict):
        stock_code = stock['代码']
        stock_name = stock['名称']

        # # 为了方便调试，开启以下功能
        # if debug and keyboard.is_pressed('enter'):
        #     print(f"继续按回车键1秒跳过接口：{basic_name}")
        #     time.sleep(1)
        #     if keyboard.is_pressed('enter'):
        #         print(f"强制跳过接口：{basic_name}")
        #         return
        if debug and i > 400:
            return

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
            market = "深A股" if flag else "沪A股"
            # # 写入的文件路径
            if individual_file:
                targeted_filepath = os.path.join(company_base_path, market, company_name_safe)
            else:
                targeted_filepath = os.path.join(company_base_path, market)  # 个股信息存储的路径
            os.makedirs(os.path.join(targeted_filepath, company_name_safe), exist_ok=True)
            filepath = os.path.join(targeted_filepath, f"{company_name_safe}_{basic_name}_{report_date}.json")

            interface_df = load_json_df(filepath)
            if interface_df.empty:
                # 通过args传递接口的其它参数
                if is_symbol:
                    interface_df = func(symbol=stock_code, **args)
                else:
                    interface_df = func(stock=stock_code, **args)

            if not isinstance(interface_df, pd.DataFrame):
                raise ValueError(f"{basic_name} does not return DataFrame ")
            if interface_df.empty:
                print(f"Fail to fetch {stock_name}  {stock_code} data，interface：{basic_name}")
                error_reports.append({"stock_name": stock_name, "stock_code": stock_code, "flag": flag,
                                      "Error": f"From {basic_name}m empty dataframe"})
                continue
            # 确保日期字段转换为字符串格式
            for col in interface_df.columns:
                if pd.api.types.is_datetime64_any_dtype(interface_df[col]):
                    interface_df[col] = interface_df[col].apply(lambda x: x.isoformat() if pd.notnull(x) else None)
                elif pd.api.types.is_object_dtype(interface_df[col]):
                    interface_df[col] = interface_df[col].astype(str)

            # 记录已处理的股票
            processed_stocks.add(stock_code)
            save_to_json_v2(interface_df, filepath)
            count = 0
            for index, row in interface_df.iterrows():
                count += 1
                if count > 1:
                    print(f"Error processing stock {stock_code}: input file interface_df has more than one row")
                    error_reports.append(
                        {"stock_name": stock_name, "stock_code": stock_code, "flag": flag, "Error": "input file interface_df has more than one row"})
                pct_change = row["涨跌幅"]
                if pct_change > 0:
                    df_up_stock_dict = pd.concat(
                        [df_up_stock_dict, pd.DataFrame([{"代码": stock_code, "名称": stock_name}])],
                        ignore_index=True)
                if 1 <= pct_change < 3:
                    df_up_range13 = pd.concat(
                        [df_up_range13, pd.DataFrame([{"代码": stock_code, "名称": stock_name}])],
                        ignore_index=True)
                elif 3 <= pct_change < 5:
                    df_up_range35 = pd.concat(
                        [df_up_range35, pd.DataFrame([{"代码": stock_code, "名称": stock_name}])],
                        ignore_index=True)
                elif 5 <= pct_change < 7:
                    df_up_range57 = pd.concat(
                        [df_up_range57, pd.DataFrame([{"代码": stock_code, "名称": stock_name}])],
                        ignore_index=True)
                elif 7 <= pct_change < 10:
                    df_up_range710 = pd.concat(
                        [df_up_range710, pd.DataFrame([{"代码": stock_code, "名称": stock_name}])],
                        ignore_index=True)
                elif pct_change >= 10:
                    df_up_range10 = pd.concat(
                        [df_up_range10, pd.DataFrame([{"代码": stock_code, "名称": stock_name}])],
                        ignore_index=True)
                processed_data = pd.concat([processed_data, pd.DataFrame(
                    [{"代码": stock_code, "名称": stock_name, "涨幅": pct_change}])], ignore_index=True)
            # 定期保存中间结果和中断点
            if (i + 1) % 10 == 0 or i == total_stocks - 1:
                if get_full_file:
                    save_to_json_v2(processed_data, data_file)
                save_to_json({"processed_stocks": list(processed_stocks)}, interrupt_file)
                save_to_json(error_reports, error_file)
                if (i + 1) % frequency == 0:
                    print(f"Progress: {i + 1}/{total_stocks} stocks processed.")

        except Exception as e:
            print(f"Error processing stock {stock_code}: {e}")
            error_reports.append({"stock_name": stock_name, "stock_code": stock_code, "flag": flag, "Error": str(e)})
            if (i + 1) % 10 == 0 or i == total_stocks - 1:
                if get_full_file:
                    save_to_json_v2(processed_data, data_file)
                save_to_json({"processed_stocks": list(processed_stocks)}, interrupt_file)
                save_to_json(error_reports, error_file)

                save_to_json_v2(df_up_stock_dict, up_stock_dict_file)
                save_to_json_v2(df_up_range13, up_range13_file)
                save_to_json_v2(df_up_range35, up_range35_file)
                save_to_json_v2(df_up_range57, up_range57_file)
                save_to_json_v2(df_up_range710, up_range710_file)
                save_to_json_v2(df_up_range10, up_range10_file)
                if (i + 1) % frequency == 0:
                    print(f"Progress: {i + 1}/{total_stocks} stocks processed.")
            continue

        # 保存最终结果
        if get_full_file:
            save_to_json_v2(processed_data, data_file)
        save_to_json({"processed_stocks": list(processed_stocks)}, interrupt_file)
        save_to_json(error_reports, error_file)

        # 保存所有股票的报告
        # 保存分别上涨三、四、五天的报告
        if not df_up_stock_dict.empty:
            save_to_json_v2(df_up_stock_dict, up_stock_dict_file)
        if not df_up_range13.empty:
            save_to_json_v2(df_up_range13, up_range13_file)
        if not df_up_range35.empty:
            save_to_json_v2(df_up_range35, up_range35_file)
        if not df_up_range57.empty:
            save_to_json_v2(df_up_range57, up_range57_file)
        if not df_up_range710.empty:
            save_to_json_v2(df_up_range710, up_range710_file)
        if not df_up_range10.empty:
            save_to_json_v2(df_up_range10, up_range10_file)


def get_weekly_reports(date, report_date, base_path='./stock_data'):
    print("Now executing function: get_weekly_reports")
    company_base_path = os.path.join(base_path, "company_data")
    # 输入日期，基本路径（存到./weekly_report文件夹中）
    begin_date, end_date = get_week_range(date)
    func = ak.stock_zh_a_hist
    # 正确的字典形式
    args = {
        'period': 'daily',
        'start_date': begin_date,
        'end_date': end_date,
        'adjust': 'qfq'
    }
    sh_dict, sz_dict, h_dict = create_dict(base_path,False)
    print("Now traversal sh_dict \n")
    get_weekly_report(func, "weekly_report", sh_dict, 1, args, company_base_path, report_date, individual_file=True)
    print("Now traversal sz_dict \n")
    get_weekly_report(func, "weekly_report", sz_dict, 0, args, company_base_path, report_date, individual_file=True)
    print("Successfully executing function get_weekly_reports")
# get_weekly_reports("20240726", "20240726")

def check_daily_up_interface(date, base_path='./stock_data', creating_new_dict=True):
    """
    调用check_daily_up接口
    :param date:
    :param base_path:
    :param creating_new_dict:
    :return:
    """
    print("Now executing function: check_daily_up_interface")
    company_base_path = os.path.join(base_path, "company_data")
    func = ak.stock_zh_a_hist
    args = {
        'period': 'daily',
        'start_date': date,
        'end_date': date,
        'adjust': ''
    }
    if creating_new_dict:
        exclude_new_stock_interface(date, base_path)   # 调用排除新股的接口，该结构会新建词典的
        # 读取生成的新股文件
        sh_dict_path = os.path.join(company_base_path, "sh_a_stocks_excluding_new.json")
        sz_dict_path = os.path.join(company_base_path, "sz_a_stocks_excluding_new.json")
        sh_dict = load_json(sh_dict_path)
        sz_dict = load_json(sz_dict_path)
        exclude_limit_up(sh_dict, sz_dict, date, company_base_path)
        sh_dict = load_json(os.path.join(company_base_path, "sh_a_stocks_excluding_new_and_limit_up.json"))
        sz_dict = load_json(os.path.join(company_base_path, "sz_a_stocks_excluding_new_and_limit_up.json"))
    else:
        sh_dict = load_json(os.path.join(company_base_path, "sh_a_stocks_excluding_new_and_limit_up.json"))
        sz_dict = load_json(os.path.join(company_base_path, "sz_a_stocks_excluding_new_and_limit_up.json"))
        if len(sh_dict) == 0 or len(sz_dict) == 0:  # 上面if分支的代码串
            exclude_new_stock_interface(date, company_base_path)
            sh_dict_path = os.path.join(company_base_path, "sh_a_stocks_excluding_new.json")
            sz_dict_path = os.path.join(company_base_path, "sz_a_stocks_excluding_new.json")
            sh_dict = load_json(sh_dict_path)
            sz_dict = load_json(sz_dict_path)
            exclude_limit_up(sh_dict, sz_dict, date, company_base_path)
            sh_dict = load_json(os.path.join(company_base_path, "sh_a_stocks_excluding_new_and_limit_up.json"))
            sz_dict = load_json(os.path.join(company_base_path, "sz_a_stocks_excluding_new_and_limit_up.json"))

    get_limit_up_dict_v2(date, company_base_path)
    check_daily_up(func, "daily_up_report", sh_dict, 1, args, company_base_path, date, individual_file=True)
    check_daily_up(func, "daily_up_report", sz_dict, 0, args, company_base_path, date, individual_file=True)
    print("Successfully executing function check_daily_up_interface")

def get_limit_up_dict_v2(date, company_base_path='./stock_data/company_data', excluding=True, relative_path='weekly_report'):
    """
    将沪深的涨停股分离，存在两个文件中。
    :param date: 调用涨停股的日期输入，也作为文件日期后缀
    :param company_base_path: 基本路径
    :param excluding: 是否排除创新板和创业板
    :param relative_path: 基于基本路径的相对路径。由于使用os.path.join，请省略相对路径的'./'
    :return: 直接将结果写入文件
    """
    limit_up_df_sz = pd.DataFrame(columns=['代码', '名称'])
    limit_up_df_sh = pd.DataFrame(columns=['代码', '名称'])

    report_date = date
    stock_zt_pool_em_df = ak.stock_zt_pool_em(date=report_date)

    if stock_zt_pool_em_df.empty:
        print("Error! In get_limit_up_dict, fail to fetch ak.stock_zt_pool_em!")
        return

    # 定义前缀
    sh_include_prefix = ["600", "601", "603", "605", "688"]
    sz_include_prefix = ["000", "001", "002", "003", "300", "301"]

    if excluding:
        sh_include_prefix = ["600", "601", "603", "605"]
        sz_include_prefix = ["000", "001", "002", "003"]

    for index, row in stock_zt_pool_em_df.iterrows():
        stock_code = row['代码']
        stock_name = row['名称']

        if any(stock_code.startswith(prefix) for prefix in sh_include_prefix):
            limit_up_df_sh = pd.concat([limit_up_df_sh, pd.DataFrame([{"代码": stock_code, "名称": stock_name}])],
                                       ignore_index=True)
        elif any(stock_code.startswith(prefix) for prefix in sz_include_prefix):
            limit_up_df_sz = pd.concat([limit_up_df_sz, pd.DataFrame([{"代码": stock_code, "名称": stock_name}])],
                                       ignore_index=True)

    # 保存 DataFrame 为 JSON 文件
    sh_file_path = os.path.join(company_base_path, relative_path, f"limit_up_dict_sh_{report_date}.json")
    sz_file_path = os.path.join(company_base_path, relative_path, f"limit_up_dict_sz_{report_date}.json")

    save_to_json_v2(limit_up_df_sh, sh_file_path)
    save_to_json_v2(limit_up_df_sz, sz_file_path)



def exclude_limit_up(sh_dict, sz_dict, date, company_base_path='./stock_data/company_data', relative_path='weekly_report', exclude_new=True):
        """
        排除新股exclude_new=True时，将结果写入sh_a_stocks_excluding_new_and_limit_up.json与sz_a_stocks_excluding_new_and_limit_up.json，
        为False时，写入sh_a_stocks_excluding_limit_up.json与sz_a_stocks_excluding_limit_up.json
        :param sh_dict:
        :param sz_dict:
        :param date:
        :param company_base_path:
        :param exclude_new: 是否在此前排除了新股，影响输出的文件命名
        :return:
        """
        filtered_sh_df = pd.DataFrame(sh_dict)
        filtered_sz_df = pd.DataFrame(sz_dict)

        # 获取新股后的涨停股列表
        get_limit_up_dict_v2(date, company_base_path)
        sh_file_path = os.path.join(company_base_path, relative_path, f"limit_up_dict_sh_{date}.json")
        sz_file_path = os.path.join(company_base_path, relative_path, f"limit_up_dict_sz_{date}.json")
        stock_zt_sh = load_json_df(sh_file_path)
        stock_zt_sz = load_json_df(sz_file_path)

        # 提取涨停股的代码
        limit_up_codes_sh = stock_zt_sh['代码'].tolist()
        limit_up_codes_sz = stock_zt_sz['代码'].tolist()

        # 从排除新股后的数据中进一步排除涨停股
        filtered_sh_df = filtered_sh_df[~filtered_sh_df['代码'].isin(limit_up_codes_sh)]
        filtered_sz_df = filtered_sz_df[~filtered_sz_df['代码'].isin(limit_up_codes_sz)]

        if exclude_new:
            save_to_json_v2(filtered_sh_df, os.path.join(company_base_path, "sh_a_stocks_excluding_new_and_limit_up.json"))
            save_to_json_v2(filtered_sz_df, os.path.join(company_base_path, "sz_a_stocks_excluding_new_and_limit_up.json"))
        else:
            save_to_json_v2(filtered_sh_df, os.path.join(company_base_path, "sh_a_stocks_excluding_limit_up.json"))
            save_to_json_v2(filtered_sz_df, os.path.join(company_base_path, "sz_a_stocks_excluding_limit_up.json"))

def exclude_new_stock(sh_dict, sz_dict, company_base_path='./stock_data/company_data'):
    """
    排除沪深A股字典的新股，将结果写入sh_a_stocks_excluding_new.json与sz_a_stocks_excluding_new.json
    :param sh_dict: 读取已有或新获取的沪市A股字典
    :param sz_dict: 读取已有或新获取的深市A股字典
    :param company_base_path: 存储文件的路径
    :return: 直接将结果写入sh_a_stocks_excluding_new.json与sz_a_stocks_excluding_new.json
    """
    # 获取新股列表的 DataFrame
    stock_zh_a_new_em_df = ak.stock_zh_a_new_em()

    # 提取新股的代码
    new_stock_codes = stock_zh_a_new_em_df['代码'].tolist()

    # 转换 sh_dict 和 sz_dict 为 DataFrame
    sh_df = pd.DataFrame(sh_dict)
    sz_df = pd.DataFrame(sz_dict)

    # 排除新股代码
    filtered_sh_df = sh_df[~sh_df['代码'].isin(new_stock_codes)]
    filtered_sz_df = sz_df[~sz_df['代码'].isin(new_stock_codes)]

    save_to_json_v2(filtered_sh_df, os.path.join(company_base_path, "sh_a_stocks_excluding_new.json"))
    save_to_json_v2(filtered_sz_df, os.path.join(company_base_path, "sz_a_stocks_excluding_new.json"))

    # # 将 DataFrame 转换回字典列表
    # filtered_sh_dict = filtered_sh_df.to_dict(orient='records')
    # filtered_sz_dict = filtered_sz_df.to_dict(orient='records')
    #
    # print(filtered_sh_dict)
    # print(filtered_sz_dict)

def exclude_new_stock_interface(date, base_path='./stock_data'):
    company_base_path = os.path.join(base_path, "company_data")
    sh_dict, sz_dict, h_dict = create_dict(base_path, False)
    exclude_new_stock(sh_dict, sz_dict, company_base_path)


if __name__ == "__main__":
    debug = False
    report_date = "20240809"
    if debug:
        company_base_path = "./stock_data/company_data"
    else:
        company_base_path = "../data/stock_data/company_data"

    # exclude_new_stock_interface(report_date)

    # sh_dict_path = os.path.join(company_base_path, "sh_a_stocks_excluding_new.json")
    # sz_dict_path = os.path.join(company_base_path, "sz_a_stocks_excluding_new.json")
    # sh_dict = load_json_df(sh_dict_path)
    # sz_dict = load_json_df(sz_dict_path)
    # get_limit_up_dict_v2(report_date)
    # exclude_limit_up(sh_dict, sz_dict, report_date)

    # get_limit_up_dict(report_date)
    check_daily_up_interface(report_date, creating_new_dict=False)

    week_report_date = "20240810"
    get_weekly_reports(week_report_date, report_date)
