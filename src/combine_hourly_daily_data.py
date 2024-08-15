import os
import json
import numpy as np
import pandas as pd
import akshare as ak
import inspect
import keyboard
import time
from datetime import date, datetime, timedelta, time
from basic_func import DateEncoder
from basic_func import save_to_json
from basic_func import save_to_json_v2
from basic_func import load_json
from basic_func import load_json_df
from basic_func import get_yesterday
from basic_func import processing_date
from basic_func import find_latest_file
from basic_func import find_latest_file_v2
from basic_func import get_matching_h_stocks
from basic_func import create_dict
from basic_func import is_holiday
from basic_func import is_weekend



def find_filepath_to_dict(name_prefix, base_directory, begin_date=None, end_date=None):
    file_dict = {}

    # Convert begin_date and end_date to datetime objects
    if begin_date:
        begin_date = datetime.strptime(begin_date, "%Y%m%d")
    if end_date:
        end_date = datetime.strptime(end_date, "%Y%m%d")
        end_date = datetime.combine(end_date, time(23, 59, 59))

    # Walk through the base directory
    for root, dirs, files in os.walk(base_directory):
        for file in files:
            if file.startswith(name_prefix):
                try:
                    # Split the filename into parts
                    parts = file.split('_')

                    # Check if there is an underscore separating date and time
                    if len(parts[-1].split('.')[0]) == 6:  # If last part is HHMMSS
                        date_str = parts[-2]  # YYYYMMDD
                        time_str = parts[-1].split('.')[0]  # HHMMSS
                        parsed_date = datetime.strptime(date_str, "%Y%m%d")
                        parsed_time = datetime.strptime(time_str, "%H%M%S").time()
                        datetime_combined = datetime.combine(parsed_date, parsed_time)
                        is_minute_data = True  # This is time-minute data
                    else:  # If only YYYYMMDD
                        date_str = parts[-1].split('.')[0]
                        datetime_combined = datetime.strptime(date_str, "%Y%m%d")
                        is_minute_data = False  # This is day-level data

                    # Filter by date range
                    if (begin_date and datetime_combined < begin_date) or (end_date and datetime_combined > end_date):
                        continue

                    file_path = os.path.join(root, file)
                    file_dict[datetime_combined] = {'path': file_path, 'is_minute_data': is_minute_data}

                except ValueError as e:
                    print(f"Error processing file '{file}': {e}")
                    continue

    # Sort the dictionary by date and time
    sorted_dict = dict(sorted(file_dict.items()))

    return sorted_dict


def process_dict(file_dict, output_file, indices=None):
    all_dfs = []
    if 37 in indices:  # 注：240812版本后，时分数据多获取了一次开市前的数据。如果有这个数据，输入indices3，否则还是indices2
        remainder = 39
    else:
        remainder = 38

    # Traverse the sorted dictionary
    for idx, (datetime_key, info) in enumerate(sorted(file_dict.items())):
        file_path = info['path']
        is_minute_data = info['is_minute_data']

        # If indices are provided and the data is time-minute data
        if indices and is_minute_data:
            if (idx + 1) % remainder not in indices:  # Skip if index is not in the predefined list
                continue

        # Read the JSON file into a DataFrame
        try:
            # Load JSON file content
            with open(file_path, 'r', encoding='utf-8') as file:
                json_data = json.load(file)

            # Convert JSON to DataFrame
            if isinstance(json_data, list):
                df = pd.DataFrame(json_data)
            elif isinstance(json_data, dict):
                df = pd.DataFrame([json_data])
            else:
                print(f"Skipping file '{file_path}' due to incompatible format.")
                continue

            # Check if DataFrame is empty
            if df.empty:
                print(f"Skipping file '{file_path}' due to empty DataFrame.")
                continue

            df['日期'] = datetime_key.strftime('%Y-%m-%d')
            if is_minute_data:
                df['具体时间'] = datetime_key.strftime('%H:%M:%S')

            # Add the DataFrame to the list
            all_dfs.append(df)
        except Exception as e:
            print(f"Error reading file '{file_path}': {e}")
            continue

    if all_dfs:
        # Merge all DataFrames
        merged_df = pd.concat(all_dfs, ignore_index=True)

        # Save the merged DataFrame to a JSON file
        save_to_json_v2(merged_df, output_file)
    else:
        print("No data to merge.")
        return pd.DataFrame()

    return merged_df
#
# def test():
#     # Example usage:
#     base_directory = "E:/Project_storage/stock_data/company_data/沪A股/艾华集团"
#     base_directory2 = "E:/Project_storage/stock_data/company_data/沪A股/中科通达"
#     name_prefix = "adjusted_艾华集团_data_realtime"
#     name_prefix2 = "中科通达_daily_report"
#     begin_date = '20240806'
#     end_date = '20240807'
#     indices = [1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38]
#     indices2 = [0, 1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 20, 22, 24, 26, 28, 30, 32, 34, 36]
#     indices3 = [0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 21, 23, 25, 27, 29, 31, 33, 35, 37]
#
#     output_file = "./分析/output_test.json"
#     output_file2 = "./分析/output_test2.json"
#     file_dict = find_filepath_to_dict(name_prefix, base_directory, begin_date, end_date)
#     file_dict2 = find_filepath_to_dict(name_prefix2, base_directory2, begin_date, end_date)
#
#     # Print the sorted file paths with their types
#     for key, value in file_dict.items():
#         print(f"DateTime: {key}, FilePath: {value['path']}, IsMinuteData: {value['is_minute_data']}")
#     for key, value in file_dict2.items():
#         print(f"DateTime: {key}, FilePath: {value['path']}, IsMinuteData: {value['is_minute_data']}")
#
#     process_dict(file_dict, output_file, indices=indices2)
#     process_dict(file_dict2, output_file2, indices=indices)


def access_combine_data(base_directory, name_prefix, begin_date, end_date, output_filepath):
    indices_10min = [0, 1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 20, 22, 24, 26, 28, 30, 32, 34, 36]
    indices_10min_v2 = [0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 21, 23, 25, 27, 29, 31, 33, 35, 37]
    file_dict = find_filepath_to_dict(name_prefix, base_directory, begin_date, end_date)
    process_dict(file_dict, output_filepath, indices=indices_10min_v2)


def interface_combine_traversal_module(basic_name, stock_dict, flag, name_prefix_option, output_base_path=None, begin_date=None, end_date=None, company_base_path='./stock_data/company_data', report_date=get_yesterday(), get_full_file=False, individual_file=True):
    """
    合并已经获取到的每日数据或时分数据，直接输出到指定文件夹中
    :param basic_name:  中断与错误文件命名前缀
    :param stock_dict:  遍历的股票字典
    :param flag:   标记深或沪
    :param name_prefix_option:  要处理的数据的前缀
    :param output_base_path:  文件输出路径与名称
    :param begin_date:  起始时间
    :param end_date:   终止时间
    :param company_base_path:
    :param report_date:
    :param get_full_file:
    :param individual_file:
    :return:
    """
    debug = False
    # report_date = "20240710" # 操作标识号，默认为昨天的日期
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
    if get_full_file:
        data_file = os.path.join(company_base_path, f"{basic_name}_full_data_{report_date}.json")
        processed_data = load_json(data_file)
        if not isinstance(processed_data, list):
            processed_data = []

    # 遍历所有股票的字段
    total_stocks = len(stock_dict)
    for i, stock in enumerate(stock_dict):
        stock_code = stock['代码']
        stock_name = stock['名称']

        # 为了方便调试，开启以下功能
        if debug and keyboard.is_pressed('enter'):
            print(f"继续按回车键1秒跳过接口：{basic_name}")
            time.sleep(1)
            if keyboard.is_pressed('enter'):
                print(f"强制跳过接口：{basic_name}")
                return
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
                targeted_filepath = os.path.join(company_base_path, market, company_name_safe)
            else:
                targeted_filepath = os.path.join(company_base_path, market)  # 个股信息存储的路径


            base_directory = targeted_filepath

            for key, value in name_prefix_option.items():
                if value:
                    if key == "realtime_data":
                        if output_base_path is None:
                            output_filepath = os.path.join(targeted_filepath,
                                                           f"{company_name_safe}_data_combined_{report_date}.json")
                        else:
                            output_filepath = os.path.join(output_base_path,
                                                           f"{company_name_safe}_data_combined_{report_date}.json")
                        access_combine_data(base_directory, f"{company_name_safe}_data_2", begin_date, end_date, output_filepath)
                    elif key == "daily_report":
                        if output_base_path is None:
                            output_filepath = os.path.join(targeted_filepath,
                                                           f"{company_name_safe}_daily_report_combined_{report_date}.json")
                        else:
                            output_filepath = os.path.join(output_base_path,
                                                           f"{company_name_safe}_daily_report_combined_{report_date}.json")
                        access_combine_data(base_directory, f"{company_name_safe}_daily_report", begin_date, end_date,
                                            output_filepath)

            # 记录已处理的股票
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


def combine_hourly_daily_data(begin_date, end_date, base_path, name_prefix_option={"realtime_data": True,"daily_report": True}):
    """
    param name_prefix_option: {"realtime_data": True,"daily_report": True}
    """
    print("Now executing function: combine_hourly_daily_data")
    basic_name = "real_time_data_combined"
    if name_prefix_option is None:
        name_prefix_option = {"realtime_data": True, "daily_report": True}
    output_sh = os.path.join(base_path,"沪A股")
    output_sz = os.path.join(base_path,"深A股")
    company_base_path = os.path.join(base_path, "company_data")

    sh_dict, sz_dict, h_dict = create_dict(base_path=base_path)
    interface_combine_traversal_module(basic_name, sh_dict, 0, name_prefix_option,
                                       output_base_path=output_sh, company_base_path=company_base_path,
                                       begin_date=begin_date, end_date=end_date)
    interface_combine_traversal_module(basic_name, sz_dict, 1, name_prefix_option,
                                       output_base_path=output_sz, company_base_path=company_base_path,
                                       begin_date=begin_date, end_date=end_date)


if __name__ == "__main__":
    # is_local = True
    # if is_local:
    #     base_path = './stock_data'
    # else:
    #     current_dir = os.path.dirname(os.path.abspath(__file__))
    #     parent_dir = os.path.dirname(current_dir)
    #     base_path = os.path.join(parent_dir, 'data', 'stock_data')
    #     os.makedirs(os.path.join(parent_dir, 'data', 'stock_data'), exist_ok=True)
    #     os.makedirs(os.path.join(parent_dir, 'data', 'stock_data/company_history_data'), exist_ok=True)
    #     os.makedirs(os.path.join(parent_dir, 'data', 'stock_data/company_history_data/深A股'), exist_ok=True)
    #     os.makedirs(os.path.join(parent_dir, 'data', 'stock_data/company_history_data/沪A股'), exist_ok=True)
    #     print(base_path)

    report_date = "20240815"
    basic_name = "real_time_data_combined"
    name_prefix_option = {"realtime_data": True,
                          "daily_report": False
                          }
    # begin_date = "20240806"
    # end_date = "20240809"
    begin_date = end_date = report_date
    base_path = "E:/Project_storage/stock_data/company_data"
    base_company_path = os.path.join(base_path, "company_data")
    output_sh = "E:/Project_storage/stock_data/沪A股"
    output_sz = "E:/Project_storage/stock_data/深A股"

    sh_dict, sz_dict, h_dict = create_dict(base_path=base_path)
    interface_combine_traversal_module(basic_name, sh_dict, 0, name_prefix_option,
                                       output_base_path=output_sh, company_base_path=base_path,
                                       begin_date=begin_date, end_date=end_date)
    interface_combine_traversal_module(basic_name, sz_dict, 1, name_prefix_option,
                                       output_base_path=output_sz, company_base_path=base_path,
                                       begin_date=begin_date, end_date=end_date)
