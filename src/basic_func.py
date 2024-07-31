import os
import json
import time

import numpy as np
import pandas as pd
import akshare as ak
import inspect
from datetime import date, datetime, timedelta

import keyboard



class DateEncoder(json.JSONEncoder):
    # 用于正确将日期形式内容写入json文件
    def default(self, obj):
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()
        return super(DateEncoder, self).default(obj)


def save_to_json(data, path):
    """
    :param data: dateframe格式数据
    :param path: 存储路径
    :return: 将数据写入（指定路径中的）文件
    """
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4, cls=DateEncoder)


def save_to_json_v2(df, path):
    json_str = json.dumps(json.loads(df.to_json(orient='records')), ensure_ascii=False, indent=4, cls=DateEncoder)
    with open(path, 'w', encoding='utf-8') as f:
        f.write(json_str)


def load_json(path):
    """
    :param path: （指定路径中的）文件
    :return: dict或列表格式数据，读取失败返回空值。若要转dataframe要pd.DataFrame(~）
    """
    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f:
            content = f.read().strip()
            if not content:
                return {}
            return json.loads(content)
    return {}


def load_json_df(path):
    """
    :param path: （指定路径中的）文件
    :return:dataframe格式数据，读取失败返回空dataframe。
    """
    if not path:
        print("In load_json_df: Error: path is None or empty.")
        return pd.DataFrame()
    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f:
            return pd.DataFrame(json.load(f))
    return pd.DataFrame()


def get_yesterday():
    return (datetime.now() - timedelta(1)).strftime('%Y%m%d')


def processing_date(file):
    try:
        date_str = file.split('_')[-2]
        if not date_str[0].isdigit():
            # 只有年月日
            date_str = file.split('_')[-1]
            date_str = date_str.split('.')[0]  # 时间部分去掉后缀
            date = datetime.strptime(date_str, "%Y%m%d")
            date_hms = np.nan
        else:
            date = datetime.strptime(date_str, "%Y%m%d")
            date_hms = datetime.strptime(file.split('_')[-1].split('.')[0], "%H%M%S")
    except:
        return np.nan, np.nan
    return date, date_hms


def find_latest_file(base_directory, name_prefix, before_date=None, after_date=None):
    """
    寻找并返回前缀符合，日期最新的文件路径
    :param base_directory: 寻找文件的路径
    :param name_prefix: 文件的前缀
    :param before_date: 可选参数，形式为YYYYMMDD，输入后返回该日期之前（不含该日期）的日期最新的文件
    :param after_date: 可选参数，形式为YYYYMMDD，输入后返回该日期之后（包含该日期）的日期最新的文件
    :return: 寻找到的前缀符合，日期最新的文件路径
    """
    latest_file_path = None
    latest_date = None

    # 将 before_date 转换为 datetime 对象
    if before_date:
        try:
            before_date = datetime.strptime(before_date, "%Y%m%d")
        except ValueError:
            raise ValueError("before_date must be in YYYYMMDD format")
    if after_date:
        try:
            after_date = datetime.strptime(after_date, "%Y%m%d")
        except ValueError:
            raise ValueError("after_date must be in YYYYMMDD format")

    for root, dirs, files in os.walk(base_directory):
        for file in files:
            if file.startswith(name_prefix):
                # print(file)
                # 解析文件名中的日期部分，假设日期格式为YYYYMMDD
                try:
                    date_str = file.split('_')[-2]
                    # print(date_str)
                    if not date_str[0].isdigit():
                        date_str = file.split('_')[-1]
                        date_str = date_str.split('.')[0]  # 时间部分去掉后缀
                        # print(date_str)
                        date = datetime.strptime(date_str, "%Y%m%d")
                        # print(date)
                        # 如果指定了 before_date 且文件日期不在 before_date 之前，则跳过
                        if before_date and date >= before_date:
                            continue
                        if latest_date is None or date > latest_date:
                            latest_date = date
                            latest_file_path = os.path.join(root, file)
                    else:
                        date = datetime.strptime(date_str, "%Y%m%d")
                        # print(date)
                        # 如果指定了 before_date 且文件日期不在 before_date 之前，则跳过
                        if before_date and date >= before_date:
                            continue
                        if after_date and date < after_date:
                            continue
                        if latest_date is None or date > latest_date:
                            latest_date = date
                            latest_date_hms = datetime.strptime(file.split('_')[-1].split('.')[0], "%H%M%S")
                            latest_file_path = os.path.join(root, file)
                        elif date == latest_date:
                            date_hms = datetime.strptime(file.split('_')[-1].split('.')[0], "%H%M%S")
                            if (date_hms > latest_date_hms):
                                latest_date_hms = date_hms
                                latest_date = date
                                latest_file_path = os.path.join(root, file)
                except ValueError:
                    continue

    return latest_file_path


def find_latest_file_v2(base_directory, name_prefix, before_date=None, after_date=None):
    """
    寻找并返回前缀符合，日期最新的文件路径
    :param base_directory: 寻找文件的路径
    :param name_prefix: 文件的前缀
    :param before_date: 可选参数，形式为YYYYMMDD，输入后返回该日期之前（包含该日期）的日期最新的文件
    :param after_date: 可选参数，形式为YYYYMMDD，输入后返回该日期之后（包含该日期）的日期最新的文件
    :return: 寻找到的前缀符合，日期最新的文件路径
    """
    latest_file_path = None
    latest_date = None

    # 将 before_date 转换为 datetime 对象
    if before_date:
        try:
            before_date = datetime.strptime(before_date, "%Y%m%d")
        except ValueError:
            raise ValueError("before_date must be in YYYYMMDD format")
    if after_date:
        try:
            after_date = datetime.strptime(after_date, "%Y%m%d")
        except ValueError:
            raise ValueError("after_date must be in YYYYMMDD format")

    for root, dirs, files in os.walk(base_directory):
        for file in files:
            if file.startswith(name_prefix):
                # print(file)
                # 解析文件名中的日期部分，假设日期格式为YYYYMMDD
                try:
                    date_str = file.split('_')[-2]
                    # print(date_str)
                    if not date_str[0].isdigit():
                        date_str = file.split('_')[-1]
                        date_str = date_str.split('.')[0]  # 时间部分去掉后缀
                        # print(date_str)
                        date = datetime.strptime(date_str, "%Y%m%d")
                        # print(date)
                        # 如果指定了 before_date 且文件日期不在 before_date 之前（不包括before_date），则跳过
                        if before_date and date > before_date:
                            continue
                        if after_date and date < after_date:
                            continue
                        if latest_date is None or date > latest_date:
                            latest_date = date
                            latest_file_path = os.path.join(root, file)
                    else:
                        date = datetime.strptime(date_str, "%Y%m%d")
                        # print(date)
                        # 如果指定了 before_date 且文件日期不在 before_date 之前，则跳过
                        if before_date and date > before_date:
                            continue
                        if after_date and date < after_date:
                            continue
                        if latest_date is None or date > latest_date:
                            latest_date = date
                            latest_date_hms = datetime.strptime(file.split('_')[-1].split('.')[0], "%H%M%S")
                            latest_file_path = os.path.join(root, file)
                        elif date == latest_date:
                            date_hms = datetime.strptime(file.split('_')[-1].split('.')[0], "%H%M%S")
                            if (date_hms > latest_date_hms):
                                latest_date_hms = date_hms
                                latest_date = date
                                latest_file_path = os.path.join(root, file)
                except ValueError:
                    continue

    return latest_file_path


def stock_traversal_module(func, basic_name, stock_dict, flag, args, base_path='./stock_data/company_data',
                           report_date=get_yesterday(), get_full_file=False, individual_file=True):
    """
    获取每日报表
    :param func: 调用的接口函数
    :param basic_name: 接口的基本名称，用于给各文件命名
    :param stock_dict: 遍历的股票字典，已生成的的深A或沪A股字典
    :param flag: 1表示深A股字典，0表示沪A股字典；内部需要使用不同接口
    :param args: 接口需要的股票代码外的参数
    :param base_path: 每日报表生成的基本路径
    :param report_date: 数据储存时文件的后缀id，默认为日期，为日期时可用find_latest_date函数
    :param get_full_file: bool类型，如果要将所有遍历获取的数据额外存储在一个文件中，设为True，默认为False
    :param individual_file: bool类型，数据文件存储公司文件夹还是深沪A股的大文件夹，默认为True即存入公司文件夹
    :return: 无返回值，直接写入文件并存储
    """
    debug = True
    # report_date = "20240710" # 操作标识号，默认为昨天的日期
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
    if get_full_file:
        data_file = os.path.join(base_path, f"{basic_name}_full_data_{report_date}.json")
        processed_data = load_json(data_file)
        if not isinstance(processed_data, list):
            processed_data = []
    temp_args = inspect.signature(func).parameters
    if "symbol" in temp_args:
        is_symbol = True
    else:
        is_symbol = False

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


def get_matching_h_stocks():
    """通过A+H股票字典，将沪深A股名称与对应的H股代码和名称记录下来"""
    # 获取A+H股票字典
    ah_stock_dict = ak.stock_zh_ah_name()

    # 初始化结果列表和已记录的A股名称集合
    matching_stocks = []

    # 获取沪A股和深A股的实时行情数据
    stock_sh_a_spot_df = ak.stock_sh_a_spot_em()
    stock_sz_a_spot_df = ak.stock_sz_a_spot_em()

    # 合并沪A股和深A股的实时行情数据
    stock_a_spot_df = pd.concat([stock_sh_a_spot_df, stock_sz_a_spot_df], ignore_index=True)

    # 遍历A+H股票字典，找到对应的沪深A股并记录对应的H股信息
    for _, row in ah_stock_dict.iterrows():
        a_stock_name = row['名称']
        h_stock_code = row['代码']

        # 查找沪深A股中的匹配项
        matching_a_stocks = stock_a_spot_df[stock_a_spot_df['名称'] == a_stock_name]

        # 如果有匹配的A股信息则记录对应的H股信息
        if not matching_a_stocks.empty:
            a_row = matching_a_stocks.iloc[0]
            matching_stocks.append({
                "A股名称": a_row['名称'],
                "A股代码": a_row['代码'],
                "H股代码": h_stock_code
            })

    return matching_stocks


def create_dict(base_path='./stock_data/company_data', get_H_dict=True):
    """
    :param base_path: 基本路径，默认为'./stock_data/company_data'。同样涉及到已有文件结构。
    :param get_H_dict: True同时获取H股词典，只留下深沪股市对应的股票，False则不获取
    :return: 直接将生成的字典写入基本路径内，名称为"sh_a_stocks.json"与"sz_a_stocks.json"；返回沪、深、港的字典
    """
    # 读取沪A股和深A股的数据，并构建词典并将词典与数据传递给子函数
    stock_sh_a_spot_em_df = ak.stock_sh_a_spot_em()
    stock_sz_a_spot_em_df = ak.stock_sz_a_spot_em()
    # 提取沪A股的编号、名称和代码
    sh_a_stocks = stock_sh_a_spot_em_df[['序号', '名称', '代码']].drop_duplicates().to_dict(orient='records')
    # 提取深A股的编号、名称和代码
    sz_a_stocks = stock_sz_a_spot_em_df[['序号', '名称', '代码']].drop_duplicates().to_dict(orient='records')
    save_to_json(sh_a_stocks, os.path.join(base_path, "sh_a_stocks.json"))
    save_to_json(sz_a_stocks, os.path.join(base_path, "sz_a_stocks.json"))

    if get_H_dict:
        # 获取A+H股票字典
        ah_stock_dict = ak.stock_zh_ah_name()
        matching_stocks = []
        found_first = False
        stock_a_spot_df = pd.concat([stock_sh_a_spot_em_df, stock_sz_a_spot_em_df], ignore_index=True)
        # 遍历A+H股票字典，找到对应的沪深A股并记录对应的H股信息
        for _, row in ah_stock_dict.iterrows():
            a_stock_name = row['名称']
            h_stock_code = row['代码']
            # 查找沪深A股中的匹配项
            matching_a_stocks = stock_a_spot_df[stock_a_spot_df['名称'] == a_stock_name]
            # 如果有匹配的A股信息则记录对应的H股信息
            if not matching_a_stocks.empty:
                if not found_first:
                    # 通常情况下，matching_a_stocks只会有一条记录，因为A股名称是唯一的
                    a_row = matching_a_stocks.iloc[0]
                    matching_stocks.append({
                        "A股名称": a_row['名称'],
                        "A股代码": a_row['代码'],
                        "H股代码": h_stock_code
                    })
                    found_first = True
                elif not (a_stock_name == matching_stocks[0]['A股名称']):
                    a_row = matching_a_stocks.iloc[0]
                    matching_stocks.append({
                        "A股名称": a_row['名称'],
                        "A股代码": a_row['代码'],
                        "H股代码": h_stock_code
                    })
                else:
                    break
        save_to_json(matching_stocks, os.path.join(base_path, "szsh_H_stocks.json"))
    else:
        matching_stocks={}
    return sh_a_stocks, sz_a_stocks, matching_stocks


def get_yesterday():
    return (datetime.now() - timedelta(1)).strftime('%Y%m%d')


def is_holiday(date_str):
    holidays = [
        "20240101",  # 元旦
        "20240210", "20240211", "20240212", "20240213", "20240214", "20240215", "20240216",  # 春节
        "20240405",  # 清明节
        "20240501",  # 劳动节
        "20240609", "20240610", "20240611",  # 端午节
        "20240913", "20240914", "20240915",  # 中秋节
        "20241001", "20241002", "20241003", "20241004", "20241005", "20241006", "20241007",  # 国庆节
    ]
    return date_str in holidays


def is_weekend(date):
    return date.weekday() >= 5  # 5: Saturday, 6: Sunday