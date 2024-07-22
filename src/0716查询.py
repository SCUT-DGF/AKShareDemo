import os
import json
import numpy as np
import pandas as pd
import akshare as ak
import inspect
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

holidays = [
    "20240101",  # 元旦
    "20240210", "20240211", "20240212", "20240213", "20240214", "20240215", "20240216",  # 春节
    "20240405",  # 清明节
    "20240501",  # 劳动节
    "20240609", "20240610", "20240611",  # 端午节
    "20240913", "20240914", "20240915",  # 中秋节
    "20241001", "20241002", "20241003", "20241004", "20241005", "20241006", "20241007",  # 国庆节
]

def generate_quarters(begin_date, end_date):
    start = datetime.strptime(begin_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")
    quarters = []

    while start <= end:
        year = start.year
        month = start.month
        quarter = (month - 1) // 3 + 1
        quarter_str = f"{year}{quarter}"
        if quarter_str not in quarters:
            quarters.append(quarter_str)
        if quarter == 4:
            start = datetime(year + 1, 1, 1)
        else:
            start = datetime(year, (quarter * 3) + 1, 1)
    return quarters

def is_holiday(date_str):
    return date_str in holidays

def is_weekend(date):
    return date.weekday() >= 5  # 5: Saturday, 6: Sunday

def generate_dates(begin_date, end_date):
    # 要再加上判断是否开市的功能，可以参考自动获取数据的判断
    start = datetime.strptime(begin_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")
    dates = []

    while start <= end:
        date_str = start.strftime("%Y%m%d")
        if is_holiday(date_str) or is_weekend(start):
            start += timedelta(days=1)
            continue
        else:
            dates.append(date_str)
            start += timedelta(days=1)
    return dates

def generate_report_dates(begin_date, end_date):
    start = datetime.strptime(begin_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")
    report_dates = []

    while start <= end:
        if (start.month in [3, 12] and start.day == 31) or (start.month in [6, 9] and start.day == 30) :
            report_date_str = start.strftime("%Y%m%d")
            if report_date_str not in report_dates:
                report_dates.append(report_date_str)

        if start.month == 12 and start.day == 31:
            start = datetime(start.year + 1, 1, 1)
        else:
            start += timedelta(days=1)

    return report_dates

def generate_dates_Friday(begin_date, end_date):
    """
    :param begin_date:
    :param end_date:
    :return: 只输出未休市的周五
    """
    start = datetime.strptime(begin_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")
    dates = []

    while start <= end:
        date_str = start.strftime("%Y%m%d")
        if is_holiday(date_str) or is_weekend(start) or start.weekday() != 4:
            start += timedelta(days=1)
            continue
        else:
            dates.append(date_str)
            start += timedelta(days=1)
    return dates

def stock_lhb_detail_daily_sina(begin_date, end_date):
    all_data = pd.DataFrame()
    quarters = generate_dates(begin_date,end_date)

    for item in quarters:
        df = ak.stock_lhb_detail_daily_sina(date=item)
        if not df.empty:
            all_data = pd.concat([all_data, df], ignore_index=True)
        else:
            print(f"Warning: Interface ak.stock_lhb_detail_daily_sina: datetime{item} return empty dataframes.")
    return all_data



def search_date_lists(key, func, args, date_lists):
    """
    :param key: 文件前缀
    :param func: 词典中的接口
    :param args: 输入参数
    :param date_lists: 通过对应日期生成函数获得的序列
    :return: dataframe类型的所有数据
    """
    all_data = pd.DataFrame()
    for item in date_lists:
        df = func(date=item, **args)
        if not df.empty:
            all_data = pd.concat([all_data, df], ignore_index=True)
        else:
            print(f"Warning: Interface {key}: datetime{item} return empty dataframes.")
    return all_data



api_dict = {
    # 可单独调用
    "stock_jgdy_tj_em": (ak.stock_jgdy_tj_em, {"date": "20180928"}),  # 机构调研统计接口 开始时间
    "stock_jgdy_detail_em": (ak.stock_jgdy_detail_em, {"date": "20180928"}),  # 机构调研详细接口

    # 任意开市日期 已完成
    "stock_tfp_em": (ak.stock_tfp_em, {"date": "20240426"}),  # 停复牌信息  # 开市日
    "news_trade_notify_suspend_baidu": (ak.news_trade_notify_suspend_baidu, {"date": "20220513"}),  # 停复牌
    "news_trade_notify_dividend_baidu": (ak.news_trade_notify_dividend_baidu, {"date": "20220916"}),  # 分红派息
    "news_report_time_baidu": (ak.news_report_time_baidu, {"date": "20220514"}),  # 财报发行

    # 指定特殊日期
    "stock_gpzy_pledge_ratio_em": (ak.stock_gpzy_pledge_ratio_em, {"date": "20231020"}),  # 上市公司质押比例接口  指定交易日：未休市的周五  # 完成
    "stock_sy_yq_em": (ak.stock_sy_yq_em, {"date": "20221231"}),  # 商誉减值预期明细  # 参考网站指定的数据日期 10年-23年12.31，24年3.31
    "stock_sy_jz_em": (ak.stock_sy_jz_em, {"date": "20230331"}),  # 个股商誉减值明细  # 同上
    "stock_sy_em": (ak.stock_sy_em, {"date": "20230331"}),  # 个股商誉明细  # 参考网站指定的数据日期
    "stock_sy_hy_em": (ak.stock_sy_hy_em, {"date": "20230331"}),  # 行业商誉数据  # 参考网站指定的数据日期
    "stock_analyst_rank_em": (ak.stock_analyst_rank_em, {"year": "2022"}),  # 分析师指数排行
    "stock_report_disclosure": (ak.stock_report_disclosure, {"market": "深市", "period": "2022年报"}),  # 预约披露 今年以前

    # 报表日期 已完成
    "stock_zcfz_em": (ak.stock_zcfz_em, {"date": "20220331"}),  # 资产负债表  # choice of {"20200331", "20200630", "20200930", "20201231", "..."}; 从 20100331 开始
    "stock_lrb_em": (ak.stock_lrb_em, {"date": "20220331"}),  # 利润表 报告期*3
    "stock_xjll_em": (ak.stock_xjll_em, {"date": "20200331"}),  # 现金流量表
    "stock_fhps_em": (ak.stock_fhps_em, {"date": "20231231"}),  # 分红配送  # date是分红配送报告期
    "stock_yjkb_em": (ak.stock_yjkb_em, {"date": "20200331"}),  # 业绩快报 财报日期
    "stock_yjyg_em": (ak.stock_yjyg_em, {"date": "20190331"}),  # 业绩预告 财报日期
    "stock_yysj_em": (ak.stock_yysj_em, {"symbol": "沪深A股", "date": "20211231"}),  # 预约披露时间 报表日期
}





def call_date_range_integration(basic_name, begin_date, end_date, report_id, interrupt_file, processed_interfaces, base_path='./stock_data/stock_relative'):
    # 定义：加工日期，给不同的参数输入不同的日期
    api_dates = {  # 指定特殊日期
        # 任意开市日期
        "stock_tfp_em": (ak.stock_tfp_em, {"date": "20240426"}),  # 停复牌信息  # 开市日
        "news_trade_notify_suspend_baidu": (ak.news_trade_notify_suspend_baidu, {"date": "20220513"}),  # 停复牌
        "news_trade_notify_dividend_baidu": (ak.news_trade_notify_dividend_baidu, {"date": "20220916"}),  # 分红派息
        "news_report_time_baidu": (ak.news_report_time_baidu, {"date": "20220514"}),  # 财报发行
    }
    dates = generate_dates(begin_date,end_date)
    print(dates)
    call_date_range_v2(basic_name, api_dates, dates, 2, report_id, interrupt_file, processed_interfaces, base_path)

    api_report_dates = {
        # 报表日期
        "stock_zcfz_em": (ak.stock_zcfz_em, {"date": "20220331"}),
        # 资产负债表  # choice of {"20200331", "20200630", "20200930", "20201231", "..."}; 从 20100331 开始
        "stock_lrb_em": (ak.stock_lrb_em, {"date": "20220331"}),  # 利润表 报告期*3
        "stock_xjll_em": (ak.stock_xjll_em, {"date": "20200331"}),  # 现金流量表
        "stock_fhps_em": (ak.stock_fhps_em, {"date": "20231231"}),  # 分红配送  # date是分红配送报告期
        "stock_yjkb_em": (ak.stock_yjkb_em, {"date": "20200331"}),  # 业绩快报 财报日期
        "stock_yjyg_em": (ak.stock_yjyg_em, {"date": "20190331"}),  # 业绩预告 财报日期
        "stock_yysj_em": (ak.stock_yysj_em, {"symbol": "沪深A股", "date": "20211231"}),  # 预约披露时间 报表日期
    }
    report_dates = generate_report_dates(begin_date, end_date)
    print(report_dates)
    call_date_range_v2(basic_name, api_report_dates, report_dates, 2, report_id, interrupt_file, processed_interfaces, base_path)

    api_dates_Friday = {
        "stock_gpzy_pledge_ratio_em": (ak.stock_gpzy_pledge_ratio_em, {"date": "20231020"}),  # 上市公司质押比例接口  指定交易日：未休市的周五
    }
    dates_Friday = generate_dates_Friday(begin_date, end_date)
    call_date_range_v2(basic_name, dates_Friday, api_dates_Friday, 2, report_id, interrupt_file, processed_interfaces,
                       base_path)



def call_date_range_v2(basic_name, dict, date_lists, type, report_id, interrupt_file, processed_interfaces, base_path='./stock_data/stock_relative'):
    # type为1传入的是起止日期，为2时传入报告日期或开市日期
    error_file = os.path.join(base_path, f"{basic_name}_error_reports_{report_id}.json")
    error_reports = load_json(error_file)
    if not isinstance(error_reports, list):
        error_reports = []
    i = 0

    total_interfaces = len(dict)
    for key, (func, params) in dict.items():
        if key in processed_interfaces:
            # print(f"接口 {key} 在 {report_id}已处理，跳过 ")
            continue
        i += 1
        try:
            print(f"Now start to get {key}")
            if type == 2:
                df = search_date_lists(key, func, params, date_lists)
            print(f"successful getting {key}")
            # print(df)
            if df.empty:
                error_reports.append({"api_name": key, "error": "dataframe is empty!"})
                if (i % 2 == 0):
                    save_to_json(error_reports, error_file)
                continue
            else:
                save_to_json_v2(df, f"{base_path}/{key}_{report_id}.json")
        except Exception as e:
            print(f"When getting {key}: Error: {str(e)}")
            error_reports.append({"api_name": key, "error": str(e)})
            if (i % 2 == 0):
                save_to_json(error_reports, error_file)
            continue

        processed_interfaces.add(key)
        # 定期保存中间结果和中断点
        if i % 2 == 0 or i == total_interfaces:
            save_to_json({"processed_interfaces": list(processed_interfaces)}, interrupt_file)
            save_to_json(error_reports, error_file)
            if (i % 10 == 0):
                print(f"Progress: {i + 1}/{total_interfaces} interfaces processed.")
    # 保存最终结果
    save_to_json({"processed_interfaces": list(processed_interfaces)}, interrupt_file)
    save_to_json(error_reports, error_file)


def calling_func():
    basic_name = "stock_relative_fetch"
    fetch_date = datetime.now().strftime("%Y%m%d")
    base_path = './stock_data/stock_relative'
    os.makedirs(base_path, exist_ok=True)

    interrupt_file = os.path.join(base_path, f'{basic_name}_interrupt_{fetch_date}.json')
    interrupt_data = load_json(interrupt_file)
    if not isinstance(interrupt_data, dict):
        interrupt_data = {}
    processed_interfaces = set(interrupt_data.get('processed_interfaces', []))
    begin_date = "20240601"
    end_date = "20240716"
    report_id = "20240713"

    # call_all_functions(basic_name, api_functions, fetch_date, interrupt_file, processed_interfaces, base_path)  # 自定义周期
    # call_all_functions(basic_name, api_void, fetch_date, interrupt_file, processed_interfaces, base_path)  # 自定义周期
    # call_all_functions(basic_name, api_void_daily, fetch_date, interrupt_file, processed_interfaces, base_path)  # 5天一次
    # call_all_functions(basic_name, api_void_monthly, fetch_date, interrupt_file, processed_interfaces,
    #                    base_path)  # 月度的自定义周期
    # call_date_range(basic_name, "20240701", "20240716", fetch_date, interrupt_file, processed_interfaces, base_path)
    call_date_range_integration(basic_name, begin_date, end_date, report_id, interrupt_file, processed_interfaces,
                                base_path)

# print(ak.stock_jgdy_tj_em(date="20240701"))
# print(ak.stock_sy_em(date="20231231"))
# print(ak.stock_report_disclosure(market="深市", period="2023年报"))
# print(generate_dates("20240601", "20240716"))
# calling_func()
# print(generate_report_dates("20220601", "20240716"))

# # datetime->weekday()的使用：星期一
# begin_date = "20240717"
# datetype = datetime.strptime(begin_date, "%Y%m%d")
# print(datetype.weekday())