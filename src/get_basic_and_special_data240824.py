import json
import os
import pandas as pd
import akshare as ak
from datetime import date, datetime, timedelta
import time
from basic_func import save_to_json
from basic_func import save_to_json_v2
from basic_func import find_latest_file
from basic_func import load_json
from basic_func import stock_traversal_module
from basic_func import create_dict

import keyboard


# 遍历股票字典
def call_func_traversal(basic_name, begin_date, end_date, report_id, base_path='./stock_data/stock_relative'):
    debug = True
    # 加载中断点记录
    interrupt_file = os.path.join(base_path, f'{basic_name}_interrupt_{report_id}.json')
    interrupt_data = load_json(interrupt_file)
    if not isinstance(interrupt_data, dict):
        interrupt_data = {}
    processed_interfaces = set(interrupt_data.get('processed_interfaces', []))
    error_file = os.path.join(base_path, f"{basic_name}_error_reports_{report_id}.json")
    error_reports = load_json(error_file)
    if not isinstance(error_reports, list):
        error_reports = []
    i = 0
    # 接口调用的相关定义

    api_szsh = {  # 深A 沪A
        "stock_a_indicator_lg": {ak.stock_a_indicator_lg, {}},
        # "stock_news_em": (ak.stock_news_em, {}),  # 新闻-个股新闻
        # # "stock_info_change_name": (ak.stock_info_change_name, {}),  # 周期遍历 曾用名
        # # "stock_fund_stock_holder": (ak.stock_fund_stock_holder, {}),  # 遍历季度 基金持股
        # # "stock_main_stock_holder": (ak.stock_main_stock_holder, {}),  # 遍历季度 主要股东
        # "stock_institute_recommend_detail": (ak.stock_institute_recommend_detail, {}),  # 周期遍历 股票评级数
        # "stock_zh_vote_baidu_stock": (ak.stock_zh_vote_baidu, {"indicator": "股票"}),  # 遍历
        # "stock_comment_detail_zlkp_jgcyd_em": (ak.stock_comment_detail_zlkp_jgcyd_em, {}),
        # "stock_cyq_em": (ak.stock_cyq_em, {"adjust": "qfq"}),
        # # "stock_industry_change_cninfo": (
        # #     ak.stock_industry_change_cninfo, {"start_date": f"{begin_date}", "end_date": f"{end_date}"}),
        # # 上市公司行业归属的变动情况 上市公司行业归属的变动情况-巨潮资讯
        # "stock_share_change_cninfo": (
        #     ak.stock_share_change_cninfo, {"start_date": f"{begin_date}", "end_date": f"{end_date}"}),
        # # 公司股本变动
        # # "stock_allotment_cninfo": (
        # #     ak.stock_allotment_cninfo, {"start_date": f"{begin_date}", "end_date": f"{end_date}"}),
        # # 配股实施方案
        # # "stock_profile_cninfo": (ak.stock_profile_cninfo, {}),  # 公司概况  公司基本信息用到了
        # # "stock_ipo_summary_cninfo": (ak.stock_ipo_summary_cninfo, {}),  # 上市相关 公司基本信息用到了
        # # "stock_fhps_detail_em": (ak.stock_fhps_detail_em, {}),  # 分红配送详细信息 报告 报告级
        # # "stock_fhps_detail_ths": (ak.stock_fhps_detail_ths, {}),  # 同花顺分红详细信息 同上
        # # 巨潮资讯-首页-公告查询-信息披露公告  这个是遍历+简单参数
        # "stock_zh_a_disclosure_report_cninfo": (ak.stock_zh_a_disclosure_report_cninfo,
        #                                         {"market": "沪深京", "category": "公司治理",
        #                                          "start_date": f"{begin_date}", "end_date": f"{end_date}"}),  # 信息披露公告
        # # 巨潮资讯-首页-数据-预约披露调研 遍历+简单参数
        # "stock_zh_a_disclosure_relation_cninfo": (ak.stock_zh_a_disclosure_relation_cninfo,
        #                                           {"market": "沪深京", "start_date": f"{begin_date}",
        #                                            "end_date": f"{end_date}"}),  # 信息披露调研
        # # # 遍历历史记录
        # # # symbol – 股票代码
        # # # indicator – choice of {"总市值", "市盈率(TTM)", "市盈率(静)", "市净率", "市现率"}
        # # # period – choice of {"近一年", "近三年", "近五年", "近十年", "全部"}
        # "stock_zh_valuation_baidu_zsz_5y": (
        #     ak.stock_zh_valuation_baidu, {"indicator": "总市值", "period": "近五年"}),  # 实时，深沪
        # "stock_zh_valuation_baidu_pe_ttm_5y": (
        #     ak.stock_zh_valuation_baidu, {"indicator": "市盈率(TTM)", "period": "近五年"}),  # 实时，深沪
        # "stock_zh_valuation_baidu_pe_static_5y": (
        #     ak.stock_zh_valuation_baidu, {"indicator": "市盈率(静)", "period": "近五年"}),  # 实时，深沪
        # "stock_zh_valuation_baidu_pb_5y": (
        #     ak.stock_zh_valuation_baidu, {"indicator": "市净率", "period": "近五年"}),  # 实时，深沪
        # "stock_zh_valuation_baidu_ps_5y": (
        #     ak.stock_zh_valuation_baidu, {"indicator": "市现率", "period": "近五年"}),  # 实时，深沪
    }
    api_sz = {  # 深A
        # # "stock_share_hold_change_szse": (ak.stock_share_hold_change_szse, {}),  # 深圳
        # "stock_individual_fund_flow_sh": (ak.stock_individual_fund_flow, {"market": "sh"}),
    }
    api_sh = {  # 沪A
        # # "stock_share_hold_change_sse": (ak.stock_share_hold_change_sse, {}),  # 上海
        # "stock_individual_fund_flow_sz": (ak.stock_individual_fund_flow, {"market": "sz"}),
    }
    api_h = {  # 港A

    }

    for i in range(4):
        if i == 0:
            api_traversal = api_szsh
        elif i == 1:
            api_traversal = api_sz
        elif i == 2:
            api_traversal = api_sh
        elif i == 3:
            api_traversal = api_h
        else:
            break

        sh_dict, sz_dict, H_dict = create_dict(get_H_dict=True)
        total_interfaces = len(api_traversal)
        for key, (func, params) in api_traversal.items():
            if key in processed_interfaces:
                # print(f"接口 {key} 在 {report_id}已处理，跳过 ")
                continue

            i += 1
            # try:
            print(f"Now is {key}")
            if debug and keyboard.is_pressed('enter'):
                print(f"继续按回车键1秒跳过接口：{basic_name}")
                time.sleep(1)
                if keyboard.is_pressed('enter'):
                    print(f"强制跳过接口：{basic_name}")
                    continue

            stock_traversal_module(func=func, basic_name=key, stock_dict=sh_dict, flag=0, args=params, report_date=report_id)
            stock_traversal_module(func=func, basic_name=key, stock_dict=sz_dict, flag=1, args=params, report_date=report_id)
            # except Exception as e:
            #     print(f"When getting {key}: Error: {str(e)}")
            #     error_reports.append({"api_name": key, "error": str(e)})
            #     if (i % 2 == 0):
            #         save_to_json(error_reports, error_file)
            #     continue

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


holidays = [
    "20240101",  # 元旦
    "20240210", "20240211", "20240212", "20240213", "20240214", "20240215", "20240216",  # 春节
    "20240405",  # 清明节
    "20240501",  # 劳动节
    "20240609", "20240610", "20240611",  # 端午节
    "20240913", "20240914", "20240915",  # 中秋节
    "20241001", "20241002", "20241003", "20241004", "20241005", "20241006", "20241007",  # 国庆节
]


def get_past_years(begin_date, end_date):
    start = datetime.strptime(begin_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")
    current_year = datetime.now().year
    years = []

    while start <= end:
        year = start.year
        if year < current_year and year not in years:
            years.append(year)
        start = datetime(year + 1, 1, 1)

    return years


def generate_special_dates(begin_date, end_date):
    start = datetime.strptime(begin_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")
    current_year = datetime.now().year
    dates = []

    while start <= end:
        year = start.year
        if year < current_year:
            date_str = f"{year}1231"
        elif year == current_year:
            date_str = f"{year}0331"
        else:
            break

        if date_str not in dates:
            dates.append(date_str)

        # Move to the next year
        start = datetime(year + 1, 1, 1)

    return dates


def is_holiday(date_str):
    return date_str in holidays


def is_weekend(date):
    return date.weekday() >= 5  # 5: Saturday, 6: Sunday


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


def generate_dates(begin_date, end_date):
    start = datetime.strptime(begin_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")
    dates = []
    while start <= end:
        date_str = start.strftime("%Y%m%d")
        dates.append(date_str)
        start += timedelta(days=1)
    return dates


def generate_report_dates(begin_date, end_date):
    start = datetime.strptime(begin_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")
    report_dates = []

    while start <= end:
        if start.month in [3, 12] and start.day == 31:
            report_date_str = start.strftime("%Y%m%d")
            if report_date_str not in report_dates:
                report_dates.append(report_date_str)
            start += timedelta(weeks=12)
        elif start.month in [6, 9] and start.day == 30:
            report_date_str = start.strftime("%Y%m%d")
            if report_date_str not in report_dates:
                report_dates.append(report_date_str)
            start += timedelta(weeks=12)
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


def search_date_lists(key, func, args, date_lists, targeted_folder):
    """
    240824修改：直接存储而非返回all_data
    :param key: 文件前缀
    :param func: 词典中的接口
    :param args: 输入参数
    :param date_lists: 通过对应日期生成函数获得的序列
    :return: dataframe类型的所有数据
    """
    code = 1  # 标识是否完整获取，若有获取失败code变为0
    for item in date_lists:
        df = func(date=item, **args)
        if not df.empty:
            filename = f"{key}_{item}.json"
            filepath = os.path.join(targeted_folder, filename)
            # 日期正确转换
            for col in df.columns:
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    df[col] = df[col].apply(
                        lambda x: x.isoformat() if pd.notnull(x) else None)
                elif pd.api.types.is_object_dtype(df[col]):
                    df[col] = df[col].astype(str)
            save_to_json_v2(df, filepath)
        else:
            print(f"Warning: Interface {key}: datetime{item} return empty dataframes.")
            code = 0
    return code


def call_all_functions(basic_name, api_func_dict, report_id, interrupt_file, processed_interfaces, base_path='./stock_data/stock_relative'):
    error_file = os.path.join(base_path, f"{basic_name}_error_reports_{report_id}.json")
    error_reports = load_json(error_file)
    if not isinstance(error_reports, list):
        error_reports = []
    i = 0

    total_interfaces = len(api_func_dict)
    for key, (func, params) in api_func_dict.items():
        if key in processed_interfaces:
            # print(f"接口 {key} 在 {report_date}已处理，跳过 ")
            continue
        i += 1
        try:
            df = func(**params)
            print(f"successful getting {key}")
            # print(df)
            save_to_json_v2(df, f"{base_path}/{key}_{report_id}.json")
        except Exception as e:
            print(f"When getting {key}: Error: {str(e)}")
            error_reports.append({"api_name": key, "error": str(e)})
            if(i % 2 == 0):
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


def call_date_range_v2(basic_name, dict, date_lists, type, report_id, interrupt_file, processed_interfaces, base_path='./stock_data'):
    targeted_folder = os.path.join(base_path, "stock_relative")
    os.makedirs(targeted_folder, exist_ok=True)
    # type为1传入的是起止日期，为2时传入报告日期或开市日期
    error_file = os.path.join(targeted_folder, f"{basic_name}_error_reports_{report_id}.json")
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
                code = search_date_lists(key, func, params, date_lists, targeted_folder)
                print(f"successful getting {key}")
                if code == 0:
                    error_reports.append({"api_name": key, "error": "dataframe is empty!"})
                    if i % 2 == 0:
                        save_to_json(error_reports, error_file)
                    continue

        except Exception as e:
            print(f"When getting {key}: Error: {str(e)}")
            error_reports.append({"api_name": key, "error": str(e)})
            if i % 2 == 0:
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


def call_date_range_integration(basic_name, begin_date, end_date, report_id, interrupt_file, processed_interfaces, base_path='./stock_data'):
    # 定义：加工日期，给不同的参数输入不同的日期
    api_dates = {  # 指定特殊日期
        # 任意开市日期
        # "stock_tfp_em": (ak.stock_tfp_em, {"date": "20240426"}),  # 停复牌信息  # 开市日
        # "news_trade_notify_suspend_baidu": (ak.news_trade_notify_suspend_baidu, {"date": "20220513"}),  # 停复牌
        # "news_trade_notify_dividend_baidu": (ak.news_trade_notify_dividend_baidu, {"date": "20220916"}),  # 分红派息
        # "news_report_time_baidu": (ak.news_report_time_baidu, {"date": "20220514"}),  # 财报发行
    }
    dates = generate_dates(begin_date,end_date)
    print(dates)
    call_date_range_v2(basic_name, api_dates, dates, 2, report_id, interrupt_file, processed_interfaces, base_path)

    api_report_dates = {
        # 报表日期
        # "stock_zcfz_em": (ak.stock_zcfz_em, {}),  # 资产负债表-负债率
        # # 资产负债表  # choice of {"20200331", "20200630", "20200930", "20201231", "..."}; 从 20100331 开始
        # # "stock_lrb_em": (ak.stock_lrb_em, {"date": "20220331"}),  # 利润表 报告期*3
        # "stock_xjll_em": (ak.stock_xjll_em, {}),  # 现金流量表-现金流量
        "stock_yjbb_em": (ak.stock_yjbb_em, {}),  # 业绩报表-净资产收益率（ROE）
        # # "stock_fhps_em": (ak.stock_fhps_em, {"date": "20231231"}),  # 分红配送  # date是分红配送报告期
        # # "stock_yjkb_em": (ak.stock_yjkb_em, {"date": "20200331"}),  # 业绩快报 财报日期
        # # "stock_yjyg_em": (ak.stock_yjyg_em, {"date": "20190331"}),  # 业绩预告 财报日期
        # # "stock_yysj_em": (ak.stock_yysj_em, {"symbol": "沪深A股", "date": "20211231"}),  # 预约披露时间 报表日期
    }
    report_dates = generate_report_dates(begin_date, end_date)
    print(report_dates)
    call_date_range_v2(basic_name, api_report_dates, report_dates, 2, report_id, interrupt_file, processed_interfaces, base_path)

    api_dates_Friday = {
        # "stock_gpzy_pledge_ratio_em": (ak.stock_gpzy_pledge_ratio_em, {"date": "20231020"}),  # 上市公司质押比例接口  指定交易日：未休市的周五
    }
    dates_Friday = generate_dates_Friday(begin_date, end_date)
    call_date_range_v2(basic_name,  api_dates_Friday, dates_Friday, 2, report_id, interrupt_file, processed_interfaces,
                       base_path)


def calling_func(begin_date, end_date, report_id, base_path='./stock_data'):

    basic_name = "stock_relative_fetch"
    fetch_date = datetime.now().strftime("%Y%m%d")
    # base_path = './stock_data/stock_relative'
    os.makedirs(base_path,exist_ok=True)

    interrupt_file = os.path.join(base_path, f'{basic_name}_interrupt_{fetch_date}.json')
    interrupt_data = load_json(interrupt_file)
    if not isinstance(interrupt_data, dict):
        interrupt_data = {}
    processed_interfaces = set(interrupt_data.get('processed_interfaces', []))

    # call_func_traversal(basic_name="traversal_test", begin_date=begin_date, end_date=end_date, report_id=report_id,
    #                        base_path=os.path.join(base_path,"stock_relative"))
    call_date_range_integration(basic_name, begin_date, end_date, report_id, interrupt_file, processed_interfaces,
                                base_path=base_path)


if __name__ == "__main__":
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_file_path = os.path.join(project_root, 'conf', 'config.json')
    data_dir_path = os.path.join(project_root, 'data')
    # 读取配置文件
    with open(config_file_path, 'r') as f:
        config = json.load(f)
    base_path = config['base_path']

    # begin_date = "20200101"
    # end_date = "20240809"
    # report_id = "20240810"
    begin_date = "20230101"
    end_date = "20231231"
    report_id = "20240824"

    calling_func(begin_date, end_date, report_id, base_path)
