import json
import os
import pandas as pd
import akshare as ak
from datetime import date, datetime, timedelta

from basic_func import save_to_json
from basic_func import save_to_json_v2
from basic_func import find_latest_file
from basic_func import load_json
from basic_func import stock_traversal_module
from basic_func import create_dict

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
        if start.month in [3, 6, 9, 12] and start.day == 31:
            report_date_str = start.strftime("%Y-%m-%d")
            if report_date_str not in report_dates:
                report_dates.append(report_date_str)

        if start.month == 12 and start.day == 31:
            start = datetime(start.year + 1, 1, 1)
        else:
            start += timedelta(days=1)

    return report_dates


def call_func_traversal(basic_name, begin_date, end_date, id, base_path='./stock_data/stock_relative'):
    # 加载中断点记录
    interrupt_file = os.path.join(base_path, f'{basic_name}_interrupt_{id}.json')
    interrupt_data = load_json(interrupt_file)
    if not isinstance(interrupt_data, dict):
        interrupt_data = {}
    processed_interfaces = set(interrupt_data.get('processed_interfaces', []))
    error_file = os.path.join(base_path, f"{basic_name}_error_reports_{id}.json")
    error_reports = load_json(error_file)
    if not isinstance(error_reports, list):
        error_reports = []
    i = 0
    # 接口调用的相关定义
    api_traversal = {
        # 遍历

    }
    api_szsh = {  # 深A 沪A
        "stock_news_em": (ak.stock_news_em, {}),  # 新闻-个股新闻
        "stock_info_change_name": (ak.stock_info_change_name, {}),  # 周期遍历
        "stock_fund_stock_holder": (ak.stock_fund_stock_holder, {}),  # 遍历季度
        "stock_main_stock_holder": (ak.stock_main_stock_holder, {}),  # 遍历季度
        "stock_institute_recommend_detail": (ak.stock_institute_recommend_detail, {}),  # 周期遍历
        "stock_zh_vote_baidu_index": (ak.stock_zh_vote_baidu, {"indicator": "指数"}),  # 遍历
        "stock_zh_vote_baidu_stock": (ak.stock_zh_vote_baidu, {"indicator": "股票"}),  # 遍历
        "stock_comment_detail_zlkp_jgcyd_em": (ak.stock_comment_detail_zlkp_jgcyd_em, {}),
        # 千股千评详情 - 主力控盘 - 机构参与度
        "stock_comment_detail_zhpj_lspf_em": (ak.stock_comment_detail_zhpj_lspf_em, {}),
        # 千股千评详情 - 综合评价 - 历史评分
        "stock_comment_detail_scrd_focus_em": (ak.stock_comment_detail_scrd_focus_em, {}),
        # 千股千评详情 - 市场热度 - 用户关注指数
        "stock_comment_detail_scrd_desire_em": (ak.stock_comment_detail_scrd_desire_em, {}),  # 市场参与意愿
        "stock_comment_detail_scrd_desire_daily_em": (
            ak.stock_comment_detail_scrd_desire_daily_em, {}),
        # 日度市场参与意愿
        "stock_comment_detail_scrd_cost_em": (ak.stock_comment_detail_scrd_cost_em, {}),  # 市场成本
        "stock_cyq_em": (ak.stock_cyq_em, {"adjust": "qfq"}),

        "stock_industry_change_cninfo": (
            ak.stock_industry_change_cninfo, {"start_date": f"{begin_date}", "end_date": f"{end_date}"}),
        # 上市公司行业归属的变动情况
        "stock_share_change_cninfo": (
            ak.stock_share_change_cninfo, {"start_date": f"{begin_date}", "end_date": f"{end_date}"}),
        # 公司股本变动
        "stock_allotment_cninfo": (
            ak.stock_allotment_cninfo, {"start_date": f"{begin_date}", "end_date": f"{end_date}"}),
        # 配股实施方案
        "stock_profile_cninfo": (ak.stock_profile_cninfo, {}),  # 公司概况
        "stock_ipo_summary_cninfo": (ak.stock_ipo_summary_cninfo, {}),  # 上市相关

        "stock_fhps_detail_em": (ak.stock_fhps_detail_em, {}),  # 分红配送详细信息
        "stock_fhps_detail_ths": (ak.stock_fhps_detail_ths, {}),  # 同花顺分红详细信息
        # 遍历历史记录
        # symbol – 股票代码
        # indicator – choice of {"总市值", "市盈率(TTM)", "市盈率(静)", "市净率", "市现率"}
        # period – choice of {"近一年", "近三年", "近五年", "近十年", "全部"}
        "stock_zh_valuation_baidu_zsz_1y": (
            ak.stock_zh_valuation_baidu, {"indicator": "总市值", "period": "近一年"}),  # 实时，深沪
        "stock_zh_valuation_baidu_zsz_3y": (
            ak.stock_zh_valuation_baidu, {"indicator": "总市值", "period": "近三年"}),  # 实时，深沪
        "stock_zh_valuation_baidu_zsz_5y": (
            ak.stock_zh_valuation_baidu, {"indicator": "总市值", "period": "近五年"}),  # 实时，深沪
        "stock_zh_valuation_baidu_zsz_10y": (
            ak.stock_zh_valuation_baidu, {"indicator": "总市值", "period": "近十年"}),  # 实时，深沪
        "stock_zh_valuation_baidu_zsz_all": (
            ak.stock_zh_valuation_baidu, {"indicator": "总市值", "period": "全部"}),  # 实时，深沪
        "stock_zh_valuation_baidu_pe_ttm_1y": (
            ak.stock_zh_valuation_baidu, {"indicator": "市盈率(TTM)", "period": "近一年"}),  # 实时，深沪
        "stock_zh_valuation_baidu_pe_ttm_3y": (
            ak.stock_zh_valuation_baidu, {"indicator": "市盈率(TTM)", "period": "近三年"}),  # 实时，深沪
        "stock_zh_valuation_baidu_pe_ttm_5y": (
            ak.stock_zh_valuation_baidu, {"indicator": "市盈率(TTM)", "period": "近五年"}),  # 实时，深沪
        "stock_zh_valuation_baidu_pe_ttm_10y": (
            ak.stock_zh_valuation_baidu, {"indicator": "市盈率(TTM)", "period": "近十年"}),  # 实时，深沪
        "stock_zh_valuation_baidu_pe_ttm_all": (
            ak.stock_zh_valuation_baidu, {"indicator": "市盈率(TTM)", "period": "全部"}),  # 实时，深沪
        "stock_zh_valuation_baidu_pe_static_1y": (
            ak.stock_zh_valuation_baidu, {"indicator": "市盈率(静)", "period": "近一年"}),  # 实时，深沪
        "stock_zh_valuation_baidu_pe_static_3y": (
            ak.stock_zh_valuation_baidu, {"indicator": "市盈率(静)", "period": "近三年"}),  # 实时，深沪
        "stock_zh_valuation_baidu_pe_static_5y": (
            ak.stock_zh_valuation_baidu, {"indicator": "市盈率(静)", "period": "近五年"}),  # 实时，深沪
        "stock_zh_valuation_baidu_pe_static_10y": (
            ak.stock_zh_valuation_baidu, {"indicator": "市盈率(静)", "period": "近十年"}),  # 实时，深沪
        "stock_zh_valuation_baidu_pe_static_all": (
            ak.stock_zh_valuation_baidu, {"indicator": "市盈率(静)", "period": "全部"}),  # 实时，深沪
        "stock_zh_valuation_baidu_pb_1y": (
            ak.stock_zh_valuation_baidu, {"indicator": "市净率", "period": "近一年"}),  # 实时，深沪
        "stock_zh_valuation_baidu_pb_3y": (
            ak.stock_zh_valuation_baidu, {"indicator": "市净率", "period": "近三年"}),  # 实时，深沪
        "stock_zh_valuation_baidu_pb_5y": (
            ak.stock_zh_valuation_baidu, {"indicator": "市净率", "period": "近五年"}),  # 实时，深沪
        "stock_zh_valuation_baidu_pb_10y": (
            ak.stock_zh_valuation_baidu, {"indicator": "市净率", "period": "近十年"}),  # 实时，深沪
        "stock_zh_valuation_baidu_pb_all": (
            ak.stock_zh_valuation_baidu, {"indicator": "市净率", "period": "全部"}),  # 实时，深沪
        "stock_zh_valuation_baidu_ps_1y": (
            ak.stock_zh_valuation_baidu, {"indicator": "市现率", "period": "近一年"}),  # 实时，深沪
        "stock_zh_valuation_baidu_ps_3y": (
            ak.stock_zh_valuation_baidu, {"indicator": "市现率", "period": "近三年"}),  # 实时，深沪
        "stock_zh_valuation_baidu_ps_5y": (
            ak.stock_zh_valuation_baidu, {"indicator": "市现率", "period": "近五年"}),  # 实时，深沪
        "stock_zh_valuation_baidu_ps_10y": (
            ak.stock_zh_valuation_baidu, {"indicator": "市现率", "period": "近十年"}),  # 实时，深沪
        "stock_zh_valuation_baidu_ps_all": (
            ak.stock_zh_valuation_baidu, {"indicator": "市现率", "period": "全部"}),  # 实时，深沪
    }
    api_sz = {  # 深A
        "stock_share_hold_change_szse": (ak.stock_share_hold_change_szse, {}),  # 深圳
        "stock_individual_fund_flow_sh": (ak.stock_individual_fund_flow, {"market": "sh"}),
    }
    api_sh = {  # 沪A
        "stock_share_hold_change_sse": (ak.stock_share_hold_change_sse, {}),  # 上海
        "stock_individual_fund_flow_sz": (ak.stock_individual_fund_flow, {"market": "sz"}),
    }
    api_h = {  # 港A
        "stock_hk_fhpx_detail_ths": (ak.stock_hk_fhpx_detail_ths, {}),
        "stock_hk_valuation_baidu_zsz_1y": (
            ak.stock_hk_valuation_baidu, {"indicator": "总市值", "period": "近一年"}),  # 实时，港股
        "stock_hk_valuation_baidu_zsz_3y": (
            ak.stock_hk_valuation_baidu, {"indicator": "总市值", "period": "近三年"}),  # 实时，港股
        "stock_hk_valuation_baidu_zsz_5y": (
            ak.stock_hk_valuation_baidu, {"indicator": "总市值", "period": "近五年"}),  # 实时，港股
        "stock_hk_valuation_baidu_zsz_10y": (
            ak.stock_hk_valuation_baidu, {"indicator": "总市值", "period": "近十年"}),  # 实时，港股
        "stock_hk_valuation_baidu_zsz_all": (
            ak.stock_hk_valuation_baidu, {"indicator": "总市值", "period": "全部"}),  # 实时，港股
        "stock_hk_valuation_baidu_pe_ttm_1y": (
            ak.stock_hk_valuation_baidu, {"indicator": "市盈率(TTM)", "period": "近一年"}),  # 实时，港股
        "stock_hk_valuation_baidu_pe_ttm_3y": (
            ak.stock_hk_valuation_baidu, {"indicator": "市盈率(TTM)", "period": "近三年"}),  # 实时，港股
        "stock_hk_valuation_baidu_pe_ttm_5y": (
            ak.stock_hk_valuation_baidu, {"indicator": "市盈率(TTM)", "period": "近五年"}),  # 实时，港股
        "stock_hk_valuation_baidu_pe_ttm_10y": (
            ak.stock_hk_valuation_baidu, {"indicator": "市盈率(TTM)", "period": "近十年"}),  # 实时，港股
        "stock_hk_valuation_baidu_pe_ttm_all": (
            ak.stock_hk_valuation_baidu, {"indicator": "市盈率(TTM)", "period": "全部"}),  # 实时，港股
        "stock_hk_valuation_baidu_pe_static_1y": (
            ak.stock_hk_valuation_baidu, {"indicator": "市盈率(静)", "period": "近一年"}),  # 实时，港股
        "stock_hk_valuation_baidu_pe_static_3y": (
            ak.stock_hk_valuation_baidu, {"indicator": "市盈率(静)", "period": "近三年"}),  # 实时，港股
        "stock_hk_valuation_baidu_pe_static_5y": (
            ak.stock_hk_valuation_baidu, {"indicator": "市盈率(静)", "period": "近五年"}),  # 实时，港股
        "stock_hk_valuation_baidu_pe_static_10y": (
            ak.stock_hk_valuation_baidu, {"indicator": "市盈率(静)", "period": "近十年"}),  # 实时，港股
        "stock_hk_valuation_baidu_pe_static_all": (
            ak.stock_hk_valuation_baidu, {"indicator": "市盈率(静)", "period": "全部"}),  # 实时，港股
        "stock_hk_valuation_baidu_pb_1y": (
            ak.stock_hk_valuation_baidu, {"indicator": "市净率", "period": "近一年"}),  # 实时，港股
        "stock_hk_valuation_baidu_pb_3y": (
            ak.stock_hk_valuation_baidu, {"indicator": "市净率", "period": "近三年"}),  # 实时，港股
        "stock_hk_valuation_baidu_pb_5y": (
            ak.stock_hk_valuation_baidu, {"indicator": "市净率", "period": "近五年"}),  # 实时，港股
        "stock_hk_valuation_baidu_pb_10y": (
            ak.stock_hk_valuation_baidu, {"indicator": "市净率", "period": "近十年"}),  # 实时，港股
        "stock_hk_valuation_baidu_pb_all": (
            ak.stock_hk_valuation_baidu, {"indicator": "市净率", "period": "全部"}),  # 实时，港股
        "stock_hk_valuation_baidu_ps_1y": (
            ak.stock_hk_valuation_baidu, {"indicator": "市现率", "period": "近一年"}),  # 实时，港股
        "stock_hk_valuation_baidu_ps_3y": (
            ak.stock_hk_valuation_baidu, {"indicator": "市现率", "period": "近三年"}),  # 实时，港股
        "stock_hk_valuation_baidu_ps_5y": (
            ak.stock_hk_valuation_baidu, {"indicator": "市现率", "period": "近五年"}),  # 实时，港股
        "stock_hk_valuation_baidu_ps_10y": (
            ak.stock_hk_valuation_baidu, {"indicator": "市现率", "period": "近十年"}),  # 实时，港股
        "stock_hk_valuation_baidu_ps_all": (
            ak.stock_hk_valuation_baidu, {"indicator": "市现率", "period": "全部"}),  # 实时，港股
    }
    api_traversal = api_szsh
    sh_dict, sz_dict, H_dict = create_dict(get_H_dict=True)
    total_interfaces = len(api_traversal)
    for key, (func, params) in api_traversal.items():
        if key in processed_interfaces:
            # print(f"接口 {key} 在 {id}已处理，跳过 ")
            continue
        i += 1
        # try:
        print(f"Now is {key}")
        stock_traversal_module(func=func, basic_name=key, stock_dict=sh_dict, flag=0, args=params, report_date=id)
        stock_traversal_module(func=func, basic_name=key, stock_dict=sz_dict, flag=1, args=params, report_date=id)
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


call_func_traversal(basic_name="traversal_test", begin_date="20240713", end_date="20240713",id="20240713")