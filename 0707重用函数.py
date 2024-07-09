import os
import json
import numpy as np
import pandas as pd
import akshare as ak
from datetime import date, datetime, timedelta


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


def load_json(path):
    """
    :param path: （指定路径中的）文件
    :return: dict或dataframe格式数据，读取失败返回空值。若要转dataframe要pd.DataFrame(~）
    """
    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}


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
        return np.nan,np.nan
    return date,date_hms


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
                            if(date_hms > latest_date_hms):
                                latest_date_hms = date_hms
                                latest_date = date
                                latest_file_path = os.path.join(root, file)
                except ValueError:
                    continue

    return latest_file_path


def get_company_basic_profile(stock_dict, base_path, processed_stocks, flag, report_date, stock_sh_a_spot_em_df, stock_sz_a_spot_em_df, interrupt_file):
    """
    获取每日报表
    :param stock_dict: 已生成的的深A或沪A股字典
    :param base_path: 每日报表生成的基本路径
    :param processed_stocks: 中断处理使用，记录已处理的公司
    :param flag: 1表示深A股字典，0表示沪A股字典；内部需要使用不同接口
    :param report_date: 指定的日期
    :param stock_sh_a_spot_em_df: 沪A股数据
    :param stock_sz_a_spot_em_df: 深A股数据
    :return: 无返回值，直接写入文件并存储
    """
    basic_name = "company_basic_profile"
    data_file = os.path.join(base_path, f"{basic_name}_data_{report_date}.json")
    error_file = os.path.join(base_path, f"{basic_name}_error_reports_{report_date}.json")

    processed_data = load_json(data_file)
    error_reports = load_json(error_file)

    if not isinstance(processed_data, list):
        processed_data = []

    if not isinstance(error_reports, list):
        error_reports = []
    
    # 遍历所有股票的字段
    total_stocks = len(stock_dict)
    for i, stock in enumerate(stock_dict):
        stock_code = stock['代码']
        stock_name = stock['名称']

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
            
            if flag:
                market = "深A股"
            else:
                market = "沪A股"
            # 写入的文件路径
            company_filepath = os.path.join(base_path, market, company_name_safe,
                                             f"{company_name_safe}_{basic_name}_{datetime.now().strftime('%Y%m%d')}.json")
            os.makedirs(os.path.dirname(company_filepath), exist_ok=True)
            targeted_filepath = os.path.join(base_path, market, company_name_safe)
            
            # # 获取总股本
            # stock_individual_info_em_df = ak.stock_individual_info_em(symbol=stock_code)
            # if stock_individual_info_em_df.empty:
            #     print(f"无法获取公司 {stock_name} 代码 {stock_code} 的个股信息，对应接口：ak.stock_individual_info_em")
            #     error_reports.append({"stock_name": stock_name, "stock_code": stock_code, "flag": flag})
            #     stock_zh_a_gdhs_detail_em_df = ak.stock_zh_a_gdhs_detail_em(symbol=stock_code)
            #     if stock_zh_a_gdhs_detail_em_df.empty:
            #         print(
            #             f"无法获取公司 {stock_name} 代码 {stock_code} 的总股本信息，对应接口：ak.stock_zh_a_gdhs_detail_em")
            #         continue
            #     else:
            #         total_share_capital = \
            #             stock_zh_a_gdhs_detail_em_df[stock_zh_a_gdhs_detail_em_df['item'] == '总股本']['value'].values[0]
            # else:
            #     total_share_capital = \
            #     stock_individual_info_em_df[stock_individual_info_em_df['item'] == '总股本']['value'].values[0]

            # # 获取发行量、发行价格、发行日期
            # stock_ipo_summary_cninfo_df = ak.stock_ipo_summary_cninfo(symbol=stock_code)
            # if stock_ipo_summary_cninfo_df.empty:
            #     print(f"无法获取公司 {stock_name} 代码 {stock_code} 的上市相关-巨潮资讯，对应接口：ak.stock_ipo_summary_cninfo")
            #     error_reports.append({"stock_name": stock_name, "stock_code": stock_code, "flag": flag})
            #     continue
            # issue_size = stock_ipo_summary_cninfo_df.at[0, '总发行数量']


            report_date_str = report_date.replace("-", "").replace(":", "").replace(" ", "")
            
            # 尝试读取已有公司基本信息
            latest_file_path_1 = find_latest_file(targeted_filepath, f"{company_name_safe}_profile_")
            if latest_file_path_1:
                # print(latest_file_path_1)
                profile_df = pd.DataFrame(load_json(latest_file_path_1), index=[0])
                company_fullname = profile_df.at[0, '公司名称']
            else:
                # 错误处理
                company_fullname = stock_name

            single_data = {
                # "公司名称": company_fullname,
            }

            processed_data.append(single_data)

            # 单条数据保存为JSON文件
            save_to_json(single_data, company_filepath)

            # 记录已处理的股票
            processed_stocks.add(stock_code)

            # 定期保存中间结果和中断点
            if (i + 1) % 10 == 0 or i == total_stocks - 1:
                save_to_json(processed_data, data_file)
                save_to_json({"processed_stocks": list(processed_stocks)}, interrupt_file)
                save_to_json(error_reports, error_file)
                print(f"Progress: {i + 1}/{total_stocks} stocks processed.")

        except Exception as e:
            print(f"Error processing stock {stock_code}: {e}")
            error_reports.append({"stock_name": stock_name, "stock_code": stock_code, "flag": flag})
            continue

        # 保存最终结果
        save_to_json(processed_data, data_file)
        save_to_json({"processed_stocks": list(processed_stocks)}, interrupt_file)
        save_to_json(error_reports, error_file)


def get_processed_data(base_path='./stock_data/company_data', report_date=get_yesterday()):
    """
    :param base_path: 基本路径，默认为'./stock_data/company_data'。同样涉及到已有文件结构。
    :param report_date 指定每日报告的日期，YYYYMMDD的str，默认是昨天；若非当日闭市隔次开市前读取，市盈率一定是错的（由实时数据读取得到）
    :return: 直接将每日报告写入公司文件夹。
    """
    basic_name = "company_basic_profile"

    # 读取沪A股和深A股的数据，并构建词典并将词典与数据传递给子函数
    stock_sh_a_spot_em_df = ak.stock_sh_a_spot_em()
    stock_sz_a_spot_em_df = ak.stock_sz_a_spot_em()
    # 提取沪A股的编号、名称和代码
    sh_a_stocks = stock_sh_a_spot_em_df[['序号', '名称', '代码']].drop_duplicates().to_dict(orient='records')
    # 提取深A股的编号、名称和代码
    sz_a_stocks = stock_sz_a_spot_em_df[['序号', '名称', '代码']].drop_duplicates().to_dict(orient='records')
    save_to_json(sh_a_stocks, os.path.join(base_path, "sh_a_stocks.json"))
    save_to_json(sz_a_stocks, os.path.join(base_path, "sz_a_stocks.json"))

    # # 指定日期
    # report_date = get_yesterday()

    # 加载中断点记录
    interrupt_file = os.path.join(base_path, f'{basic_name}_interrupt_.json')
    interrupt_data = load_json(interrupt_file)
    if not isinstance(interrupt_data, dict):
        interrupt_data = {}
    processed_stocks = set(interrupt_data.get('processed_stocks', []))

    # 生成沪A股和深A股的每日报表
    get_company_basic_profile(sh_a_stocks, base_path, processed_stocks, 0, report_date, stock_sh_a_spot_em_df, stock_sz_a_spot_em_df, interrupt_file)
    get_company_basic_profile(sz_a_stocks, base_path, processed_stocks, 1, report_date, stock_sh_a_spot_em_df, stock_sz_a_spot_em_df, interrupt_file)
