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

print(ak.stock_sector_spot("新浪行业"))
print(ak.stock_sector_spot("启明星行业"))
print(ak.stock_sector_spot("概念"))
print(ak.stock_sector_spot("地域"))
print(ak.stock_sector_spot("行业"))

# 将上面的输出的label转换为词典，写一个遍历词典的函数

# 新浪行业-板块行情-成份详情 http:// finance. sina. com. cn/ stock/ sl/#area_1 :param sector: stock_sector_spot 返回的 label 值,
# choice of {"新浪行业", "概念", "地域", "行业"}; "启明星行业" 无详情 :type sector: str :return: 指定 sector 的板块详情 :rtype: pandas. DataFrame
{"stock_sector_detail": (ak.stock_sector_detail, {"sector": "hangye_ZL01"}),  # 遍历返回的参数ak.stock_sector_spot


# 遍历基金代码
# "stock_report_fund_hold_detail": (
#     ak.stock_report_fund_hold_detail, {"date": f"{generate_report_dates(begin_date, end_date)}"}),
# 需要获取基金代码


# 东方财富网-数据中心-龙虎榜单-个股龙虎榜详情
# symbol – 股票代码
# date – 查询日期; 需要通过 ak. stock_lhb_stock_detail_date_em(symbol="600077") 接口获取相应股票的有龙虎榜详情数据的日期
# flag – choice of {"买入", "卖出"}

"stock_lhb_stock_detail_em_buy": (ak.stock_lhb_stock_detail_em, {"symbol": "", "date": "", "flag": "买入"}),
"stock_lhb_stock_detail_em_sell": (ak.stock_lhb_stock_detail_em, {"symbol": "", "date": "", "flag": "卖出"}),

# 个股详情 # ak. stock_lhb_stock_detail_date_em(symbol="600077")

    # 东方财富网-数据中心-研究报告-东方财富分析师指数-东方财富分析师指数2020最新排行-分析师详情
    # 形参:
    # analyst_id – 分析师 ID, 从 ak. stock_analyst_rank_em() 获取
    # indicator – choice of {"最新跟踪成分股", "历史跟踪成分股", "历史指数"}
    "stock_analyst_detail_em": (
    ak.stock_analyst_detail_em, {"analyst_id": "11000257131", "indicator": "最新跟踪成分股"}),  # 分析师详情
    # 巨潮资讯-首页-公告查询-信息披露公告
    "stock_zh_a_disclosure_report_cninfo": (ak.stock_zh_a_disclosure_report_cninfo,
                                            {"symbol": "000001", "market": "沪深京", "category": "公司治理",
                                             "start_date": "20230619", "end_date": "20231220"}),  # 信息披露公告
    # 巨潮资讯-首页-数据-预约披露调研
    "stock_zh_a_disclosure_relation_cninfo": (ak.stock_zh_a_disclosure_relation_cninfo,
                                              {"symbol": "000001", "market": "沪深京", "start_date": "20230619",
                                               "end_date": "20231220"}),  # 信息披露调研
    # 东方财富网-数据中心-资金流向-行业资金流-xx行业个股资金流
    "stock_sector_fund_flow_summary": (
        ak.stock_sector_fund_flow_summary, {"symbol": "电源设备", "indicator": "今日"}),
    # 东方财富网-数据中心-资金流向-行业资金流-行业历史资金流
    "stock_sector_fund_flow_hist": (ak.stock_sector_fund_flow_hist, {"symbol": "电源设备"}),
    # 东方财富网-数据中心-资金流向-概念资金流-概念历史资金流
    "stock_concept_fund_flow_hist": (ak.stock_concept_fund_flow_hist, {"symbol": "锂电池"}),
    # 新浪财经-股票-机构持股详情
    "stock_institute_hold_detail": (
    ak.stock_institute_hold_detail, {"quarter": f"{generate_quarters(begin_date, end_date)}"}),
}
