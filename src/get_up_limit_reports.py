import os
import pandas as pd
import akshare as ak
from basic_func import save_to_json
from basic_func import load_json
from basic_func import load_json_df_all_scalar
from basic_func import get_yesterday
from basic_func import find_latest_file_v2
from get_daily_reports import get_daily_report
from get_weekly_report_and_daily_up2 import get_limit_up_dict_v2
from get_company_profile import get_company_basic_profile
from get_company_relative_profile import get_company_relative_profile


def merge_zt_a_data(stock_dict, processed_stocks, flag, report_date, interrupt_file, company_base_path="./stock_data/company_data", existing_data=True, debug=False):
    """
    实现需求五.3
    输出每日A-H股中上涨的A股以及对应的H股：公司名称、A股简称、股票代码、总股本、今日开盘股价、今日收盘股价（也是今日收盘后的实时数据）、
    今日开盘总市值、今日收盘总市值（公司的，至少港股没有）、今日涨跌、今日涨跌幅（stock_zh_ah_spot后的公司实时数据）、今日收盘发行市盈率
    :param stock_dict: 已生成的的深A或沪A股字典
    :param company_base_path: 公司文件夹的基本路径
    :param processed_stocks: 中断处理使用，记录已处理的公司
    :param flag: 1表示深A股字典，0表示沪A股字典；内部需要使用不同接口
    :param report_date: 指定的日期
    :return: 无返回值，直接写入文件并存储
    """
    print("Now executing function: merge_zt_a_data")
    if not existing_data:
        stock_sh_a_spot_em_df = ak.stock_sh_a_spot_em()
        stock_sz_a_spot_em_df = ak.stock_sz_a_spot_em()

    merge_zt_a_data_file = os.path.join(company_base_path, f"merge_zt_a_data_{report_date}.json")
    error_reports_file = os.path.join(company_base_path, f"merge_zt_a_data_error_reports_{report_date}.json")

    merge_zt_a_data = load_json(merge_zt_a_data_file)
    error_reports = load_json(error_reports_file)

    if not isinstance(merge_zt_a_data, list):
        merge_zt_a_data = []

    if not isinstance(error_reports, list):
        error_reports = []

    # 公司当日每日报表
    daily_report_interrupt_file = os.path.join(company_base_path, f'daily_reports_interrupt_{report_date}.json')
    daily_report_interrupt_data = load_json(daily_report_interrupt_file)
    # Ensure interrupt_data is a dictionary
    if not isinstance(daily_report_interrupt_data, dict):
        daily_report_interrupt_data = {}
    daily_report_processed_stocks = set(daily_report_interrupt_data.get('processed_stocks', []))

    # 公司基本信息
    company_profile_interrupt_file = os.path.join(company_base_path, f'company_basic_profiles_company_profile_interrupt_{report_date}.json')
    company_profile_interrupt_data = load_json(company_profile_interrupt_file)
    # Ensure company_profile_interrupt_data is a dictionary
    if not isinstance(company_profile_interrupt_data, dict):
        company_profile_interrupt_data = {}
    company_profile_processed_stocks = set(company_profile_interrupt_data.get('processed_stocks', []))

    # 公司相关信息
    company_relative_profile_interrupt_file = os.path.join(company_base_path, f'company_relative_profile_interrupt_{report_date}.json')
    company_relative_profile_interrupt_data = load_json(company_relative_profile_interrupt_file)
    # Ensure company_relative_profile_interrupt_data is a dictionary
    if not isinstance(company_relative_profile_interrupt_data, dict):
        company_relative_profile_interrupt_data = {}
    company_relative_profile_processed_stocks = set(company_relative_profile_interrupt_data.get('processed_stocks', []))

    total_stocks = len(stock_dict)
    for i, stock in enumerate(stock_dict):
        stock_code = stock['代码']
        stock_name = stock['名称']

        if debug:
            if i > 300:
                continue

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
            acquired_data_file = os.path.join(company_base_path, market, company_name_safe,
                                             f"{company_name_safe}_merge_zt_a_data.json")
            os.makedirs(os.path.dirname(acquired_data_file), exist_ok=True)
            company_file = os.path.join(company_base_path, market, company_name_safe)

            daily_report_file = os.path.join(company_file,
                                             f"{company_name_safe}_daily_report_{report_date_str}.json")
            daily_report_df = load_json_df_all_scalar(daily_report_file)
            if (not existing_data) and daily_report_df.empty:
                get_daily_report([{"代码":f"{stock_code}", "名称":f"{stock_name}"}], company_base_path, daily_report_processed_stocks,
                                 flag, report_date, stock_sh_a_spot_em_df, stock_sz_a_spot_em_df, daily_report_interrupt_file)
                daily_report_df = load_json_df_all_scalar(daily_report_file)
                if daily_report_df.empty:
                    raise ValueError("未能成功获取每日报表")
            elif daily_report_df.empty:
                raise ValueError("existing_data=True，而又没获取到已有的对应日期每日报表")
            daily_report_row = daily_report_df.iloc[0]

            company_profile_file = find_latest_file_v2(company_file, f"{company_name_safe}_company_basic_profile_)")
            # company_profile_file =  os.path.join(company_file,
            #                              f"{company_name_safe}_company_profile_{report_date_str}.json")
            company_profile_df = load_json_df_all_scalar(company_profile_file)
            if (not existing_data) and company_profile_df.empty:
                get_company_basic_profile([{"代码":f"{stock_code}", "名称":f"{stock_name}"}], company_base_path, company_profile_processed_stocks,
                                 flag, report_date, company_profile_interrupt_file)
                company_profile_file = find_latest_file_v2(company_file, f"{company_name_safe}_company_basic_profile_")
                company_profile_df = load_json_df_all_scalar(company_profile_file)
                if company_profile_df.empty:
                    raise ValueError("未能成功获取公司基本信息")
            elif company_profile_df.empty:
                raise ValueError("existing_data=True，而又没获取到已有的对应日期每日报表")
            company_profile_row = company_profile_df.iloc[0]

            # 相关字段
            company_relative_profile_file = find_latest_file_v2(company_file, f"{company_name_safe}_company_relative_profile_)")
            # company_relative_profile_file =  os.path.join(company_file,
            #                              f"{company_name_safe}_company_relative_profile_{report_date_str}.json")
            company_relative_profile_df = load_json_df_all_scalar(company_relative_profile_file)
            if (not existing_data) and company_relative_profile_df.empty:
                get_company_relative_profile([{"代码":f"{stock_code}", "名称":f"{stock_name}"}], company_base_path, company_relative_profile_processed_stocks,
                                 flag, report_date, stock_sh_a_spot_em_df, stock_sz_a_spot_em_df, company_relative_profile_interrupt_file)
                company_relative_profile_file = find_latest_file_v2(company_file, f"{company_name_safe}_company_relative_profile_")
                company_relative_profile_df = load_json_df_all_scalar(company_relative_profile_file)
                if company_relative_profile_df.empty:
                    raise ValueError("未能成功获取公司相关字段")
            elif company_relative_profile_df.empty:
                raise ValueError("existing_data=True，而又没获取到已有的对应日期每日报表")
            company_relative_profile_row = company_relative_profile_df.iloc[0]

            acquired_data = {
                "公司名称": daily_report_row["公司名称"],
                "总股本": daily_report_row["总股本"],  # 这是A股和H股共有的属性
                "股票简称": stock_name,
                "股票代码": stock_code,
                "今日开盘股价": daily_report_row["今日开盘股价"],
                "今日收盘股价": daily_report_row["今日收盘股价"],
                "今日开盘总市值": daily_report_row["今日开盘总市值"],
                "今日收盘总市值": daily_report_row["今日收盘总市值"],
                "今日涨跌": daily_report_row["今日涨跌"],
                "今日涨跌幅": daily_report_row["今日涨跌幅"],
                "今日收盘市盈率": daily_report_row["今日收盘发行市盈率"],
                "所属区域": company_profile_row["所属区域"],
                "主营业务": company_profile_row["主营业务"],
                "经营范围": company_profile_row.get("经营范围"),
                "公司简介": company_profile_row.get("公司简介"),
                "发行量": company_profile_row.get("发行量"),
                "发行价格": company_profile_row.get("发行价格"),
                "发行日期": company_profile_row.get("发行日期"),
                "注册资本": company_profile_row.get("注册资本"),
                "成立日期": company_profile_row.get("成立日期"),
                "近三年净利润": company_relative_profile_row.get("近三年净利润"),
                "近三年营业收入": company_relative_profile_row.get("近三年营业收入"),
                "近三年资产负债率": company_relative_profile_row.get("近三年资产负责率"),
                # "所属概念": company_relative_profile_row.get("所属概念"),
                "所属行业": company_relative_profile_row.get("所属行业"),
                # "近一个月公告": company_relative_profile_row.get("近一个月公告"),
                # "员工人数": company_relative_profile_row.get("员工人数"),
                # "管理层人数": company_relative_profile_row.get("管理层人数"),
                # "与之相关的舆情信息": company_relative_profile_row.get("与之相关的舆情信息")
            }

            merge_zt_a_data.append(acquired_data)

            # 将每日报表保存为JSON文件
            save_to_json(acquired_data, acquired_data_file)

            # 记录已处理的股票
            processed_stocks.add(stock_code)

            # 定期保存中间结果和中断点
            if (i + 1) % 10 == 0 or i == total_stocks - 1:
                save_to_json(merge_zt_a_data, merge_zt_a_data_file)
                save_to_json({"processed_stocks": list(processed_stocks)}, interrupt_file)
                save_to_json(error_reports, error_reports_file)
                print(f"Progress: {i + 1}/{total_stocks} stocks processed.")

        except Exception as e:
            print(f"Error processing stock {stock_code}: {e}")
            error_reports.append({"stock_name": stock_name, "stock_code": stock_code, "flag": flag})
            continue

        # 保存最终结果
        save_to_json(merge_zt_a_data, merge_zt_a_data_file)
        save_to_json({"processed_stocks": list(processed_stocks)}, interrupt_file)
        save_to_json(error_reports, error_reports_file)


def get_merge_zt_a_data(base_path='./stock_data', report_date=get_yesterday()):
    """
    A-H涨停A股总输出报表
    :param base_path: 基本路径，默认为'./stock_data'。
    :param report_date 指定每日报告的日期，YYYYMMDD的str，默认是昨天
    :return:
    """
    print("Now executing function: get_merge_zt_a_data")
    company_base_path = os.path.join(base_path, "company_data")
    weekly_report_path = os.path.join(company_base_path, "weekly_report")
    sh_file_path = os.path.join(weekly_report_path, f"limit_up_dict_sh_{report_date}.json")
    sz_file_path = os.path.join(weekly_report_path, f"limit_up_dict_sz_{report_date}.json")
    stock_zt_sh = load_json(sh_file_path)
    stock_zt_sz = load_json(sz_file_path)
    if len(stock_zt_sh) == 0 or len(stock_zt_sz) == 0:
        get_limit_up_dict_v2(date=report_date, company_base_path=company_base_path)
        stock_zt_sh = load_json(sh_file_path)
        stock_zt_sz = load_json(sz_file_path)
    
    # 魔改了的文件路径 company_data_filepath
    # company_data_filepath = "E:/Project_storage/stock_data/company_data"
    company_data_filepath = os.path.join(base_path, "company_data")

    # 加载中断点记录
    interrupt_file = os.path.join(company_data_filepath, f'merge_zt_a_data_interrupt_{report_date}.json')
    interrupt_data = load_json(interrupt_file)
    # Ensure interrupt_data is a dictionary
    if not isinstance(interrupt_data, dict):
        interrupt_data = {}
    processed_stocks = set(interrupt_data.get('processed_stocks', []))

    merge_zt_a_data(stock_zt_sh, processed_stocks, 0, report_date, interrupt_file, company_data_filepath, False)
    merge_zt_a_data(stock_zt_sz, processed_stocks, 1, report_date, interrupt_file, company_data_filepath, False)


def format_date_form(df):
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].apply(
                lambda x: x.isoformat() if pd.notnull(x) else None)
        elif pd.api.types.is_object_dtype(df[col]):
            df[col] = df[col].astype(str)


if __name__ == "__main__":
    is_local = True
    if is_local:
        # base_path = './stock_data'
        base_path = r"E:\Project_storage\stock_data"
    else:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        parent_dir = os.path.dirname(current_dir)
        base_path = os.path.join(parent_dir, 'data', 'stock_data')
        os.makedirs(os.path.join(parent_dir, 'data', 'stock_data'), exist_ok=True)
        os.makedirs(os.path.join(parent_dir, 'data', 'stock_data/company_data'), exist_ok=True)
        os.makedirs(os.path.join(parent_dir, 'data', 'stock_data/company_data/深A股'), exist_ok=True)
        os.makedirs(os.path.join(parent_dir, 'data', 'stock_data/company_data/沪A股'), exist_ok=True)
        os.makedirs(os.path.join(parent_dir, 'data', 'stock_data/company_data/weekly_report'), exist_ok=True)

    company_path = os.path.join(base_path, "company_data")
    weekly_report_path = os.path.join(company_path, "company_data")
    report_date = "20240812"

    get_merge_zt_a_data(report_date=report_date, base_path=base_path)  # 魔改了的文件路径 company_data_filepath，可以直接注释对应行
