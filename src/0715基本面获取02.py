import json
import os
import pandas as pd
import akshare as ak
from datetime import date, datetime, timedelta

from basic_func import save_to_json
from basic_func import save_to_json_v2
from basic_func import find_latest_file
from basic_func import load_json

# 自定义序列化器
class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()
        return super(DateEncoder, self).default(obj)

api_functions = {
    "stock_info_sh_name_code": (ak.stock_info_sh_name_code, {"symbol": "主板A股"}),
    "stock_info_sz_name_code": (ak.stock_info_sz_name_code, {"symbol": "A股列表"}),
    "stock_info_sz_change_name": (ak.stock_info_sz_change_name, {"symbol": "全称变更"}), # 周期
    "stock_info_sz_change_brief_name": (ak.stock_info_sz_change_name, {"symbol": "简称变更"}), # 周期
    # {'最新投资评级', '上调评级股票', '下调评级股票', '股票综合评级', '首次评级股票', '目标涨幅排名', '机构关注度', '行业关注度', '投资评级选股'}
    "stock_institute_recommend_level": (ak.stock_institute_recommend, {"symbol": "最新投资评级"}), # 最新，周期获取
    "stock_institute_recommend_levelup": (ak.stock_institute_recommend, {"symbol": "上调评级股票"}),
    "stock_institute_recommend_leveldown": (ak.stock_institute_recommend, {"symbol": "下调评级股票"}),
    "stock_institute_recommend_levelgeneral": (ak.stock_institute_recommend, {"symbol": "股票综合评级"}),
    "stock_institute_recommend_levelfirst": (ak.stock_institute_recommend, {"symbol": "首次评级股票"}),
    "stock_institute_recommend_riselevel": (ak.stock_institute_recommend, {"symbol": "目标涨幅排名"}),
    "stock_institute_recommend_ins_attention": (ak.stock_institute_recommend, {"symbol": "机构关注度"}),
    "stock_institute_recommend_ind_attention": (ak.stock_institute_recommend, {"symbol": "行业关注度"}),
    "stock_institute_recommend_levelselect": (ak.stock_institute_recommend, {"symbol": "投资评级选股"}),
    "stock_share_hold_change_sse_all": (ak.stock_share_hold_change_sse, {"symbol": "全部"}),
    "stock_share_hold_change_szse_all": (ak.stock_share_hold_change_szse, {"symbol": "全部"}),
    "stock_market_pe_lg_sh": (ak.stock_market_pe_lg, {"symbol": "上证"}),  #  月度，最新的实时
    "stock_market_pe_lg_sz": (ak.stock_market_pe_lg, {"symbol": "深证"}),
    "stock_market_pe_lg_cyb": (ak.stock_market_pe_lg, {"symbol": "创业板"}),
    "stock_market_pe_lg_kcb": (ak.stock_market_pe_lg, {"symbol": "科创板"}),
    # symbol – choice of {"上证50", "沪深300", "上证380", "创业板50", "中证500", "上证180", "深证红利", "深证100", "中证1000", "上证红利", "中证100", "中证800"}
    "stock_index_pe_lg_sh50": (ak.stock_index_pe_lg, {"symbol": "上证50"}),
    "stock_index_pe_lg_hs300": (ak.stock_index_pe_lg, {"symbol": "沪深300"}),
    "stock_index_pe_lg_sh380": (ak.stock_index_pe_lg, {"symbol": "上证380"}),
    "stock_index_pe_lg_cyb50": (ak.stock_index_pe_lg, {"symbol": "创业板50"}),
    "stock_index_pe_lg_zz500": (ak.stock_index_pe_lg, {"symbol": "中证500"}),
    "stock_index_pe_lg_sh180": (ak.stock_index_pe_lg, {"symbol": "上证180"}),
    "stock_index_pe_lg_szhl": (ak.stock_index_pe_lg, {"symbol": "深证红利"}),
    "stock_index_pe_lg_sz100": (ak.stock_index_pe_lg, {"symbol": "深证100"}),
    "stock_index_pe_lg_zz1000": (ak.stock_index_pe_lg, {"symbol": "中证1000"}),
    "stock_index_pe_lg_shhl": (ak.stock_index_pe_lg, {"symbol": "上证红利"}),
    "stock_index_pe_lg_zs100": (ak.stock_index_pe_lg, {"symbol": "中证100"}),
    "stock_index_pe_lg_zs800": (ak.stock_index_pe_lg, {"symbol": "中证800"}),
    "stock_market_pb_lg_sh": (ak.stock_market_pb_lg, {"symbol": "上证"}),
    "stock_market_pb_lg_sz": (ak.stock_market_pb_lg, {"symbol": "深证"}),
    "stock_market_pb_lg_cyb": (ak.stock_market_pb_lg, {"symbol": "创业板"}),
    "stock_market_pb_lg_kcb": (ak.stock_market_pb_lg, {"symbol": "科创板"}),
    # symbol – choice of {"上证50", "沪深300", "上证380", "创业板50", "中证500", "上证180", "深证红利", "深证100", "中证1000", "上证红利", "中证100", "中证800"}
    "stock_index_pb_lg_sh50": (ak.stock_index_pb_lg, {"symbol": "上证50"}),
    "stock_index_pb_lg_hs300": (ak.stock_index_pb_lg, {"symbol": "沪深300"}),
    "stock_index_pb_lg_sh380": (ak.stock_index_pb_lg, {"symbol": "上证380"}),
    "stock_index_pb_lg_cyb50": (ak.stock_index_pb_lg, {"symbol": "创业板50"}),
    "stock_index_pb_lg_zz500": (ak.stock_index_pb_lg, {"symbol": "中证500"}),
    "stock_index_pb_lg_sh180": (ak.stock_index_pb_lg, {"symbol": "上证180"}),
    "stock_index_pb_lg_szhl": (ak.stock_index_pb_lg, {"symbol": "深证红利"}),
    "stock_index_pb_lg_sz100": (ak.stock_index_pb_lg, {"symbol": "深证100"}),
    "stock_index_pb_lg_zz1000": (ak.stock_index_pb_lg, {"symbol": "中证1000"}),
    "stock_index_pb_lg_shhl": (ak.stock_index_pb_lg, {"symbol": "上证红利"}),
    "stock_index_pb_lg_zs100": (ak.stock_index_pb_lg, {"symbol": "中证100"}),
    "stock_index_pb_lg_zs800": (ak.stock_index_pb_lg, {"symbol": "中证800"}),
}

api_void={
    "stock_lhb_jgmx_sina": (ak.stock_lhb_jgmx_sina, {}),  # 实时最新
    "stock_ipo_declare": (ak.stock_ipo_declare, {}),  # 实时，最新
    "stock_register_kcb": (ak.stock_register_kcb, {}),  # 都是注册情况，最新
    "stock_register_cyb": (ak.stock_register_cyb, {}),
    "stock_register_sh": (ak.stock_register_sh, {}),
    "stock_register_sz": (ak.stock_register_sz, {}),
    "stock_register_bj": (ak.stock_register_bj, {}),
    "stock_register_db": (ak.stock_register_db, {}),
    "stock_qbzf_em": (ak.stock_qbzf_em, {}),  # 最新 无
    "stock_pg_em": (ak.stock_pg_em, {}),  # 最新，无周期

    "stock_gpzy_profile_em": (ak.stock_gpzy_profile_em, {}),  # 股权质押市场概况接口
    "stock_gpzy_pledge_ratio_detail_em": (ak.stock_gpzy_pledge_ratio_detail_em, {}),  # 重要股东股权质押明细接口
    "stock_gpzy_distribute_statistics_company_em": (ak.stock_gpzy_distribute_statistics_company_em, {}),
    # 质押机构分布统计-证券公司接口
    "stock_gpzy_distribute_statistics_bank_em": (ak.stock_gpzy_distribute_statistics_bank_em, {}),  # 质押机构分布统计-银行
    "stock_gpzy_industry_data_em": (ak.stock_gpzy_industry_data_em, {}),  # 上市公司质押比例-行业数据
    "stock_sy_profile_em": (ak.stock_sy_profile_em, {}),  # A股商誉市场概
    "stock_account_statistics_em": (ak.stock_account_statistics_em, {}),  # 股票账户统计
    "stock_comment_em": (ak.stock_comment_em, {}),  # 千股千评
    "stock_hsgt_fund_flow_summary_em": (ak.stock_hsgt_fund_flow_summary_em, {}),  # 沪深港通资金流向
    "stock_hk_ggt_components_em": (ak.stock_hk_ggt_components_em, {}),  # 港股通成份股
    "stock_fund_flow_big_deal": (ak.stock_fund_flow_big_deal, {}),
    "stock_market_fund_flow": (ak.stock_market_fund_flow, {}),

    # 日度
    "stock_dxsyl_em": (ak.stock_dxsyl_em, {}),  # 新股数据
}
api_void_daily={
    "stock_info_a_code_name": (ak.stock_info_a_code_name, {}),  # 每日
    "stock_a_high_low_statistics_all": (ak.stock_a_high_low_statistics, {"symbol": "all"}),  # 日更
    "stock_a_high_low_statistics_sz50": (ak.stock_a_high_low_statistics, {"symbol": "sz50"}),
    "stock_a_high_low_statistics_hs300": (ak.stock_a_high_low_statistics, {"symbol": "hs300"}),
    "stock_a_high_low_statistics_zz500": (ak.stock_a_high_low_statistics, {"symbol": "zz500"}),
    "stock_a_below_net_asset_statistics_all_A": (ak.stock_a_below_net_asset_statistics, {"symbol": "全部A股"}),
    "stock_a_below_net_asset_statistics_hs300": (ak.stock_a_below_net_asset_statistics, {"symbol": "沪深300"}),
    "stock_a_below_net_asset_statistics_sz50": (ak.stock_a_below_net_asset_statistics, {"symbol": "上证50"}),
    "stock_a_below_net_asset_statistics_zz500": (ak.stock_a_below_net_asset_statistics, {"symbol": "中证500"}),
    # 日更，可默认日期间隔，增加主动更新的选项
    # 形参:symbol – choice of {"近一月", "近三月", "近六月", "近一年"}
    "stock_lhb_stock_statistic_em_1month": (ak.stock_lhb_stock_statistic_em, {"symbol": "近一月"}),  # 5-10天更新，看看区别
    "stock_lhb_stock_statistic_em_3month": (ak.stock_lhb_stock_statistic_em, {"symbol": "近三月"}),
    "stock_lhb_stock_statistic_em_6month": (ak.stock_lhb_stock_statistic_em, {"symbol": "近六月"}),
    "stock_lhb_stock_statistic_em_12month": (ak.stock_lhb_stock_statistic_em, {"symbol": "近一年"}),
    "stock_lhb_jgstatistic_em_1month": (ak.stock_lhb_jgstatistic_em, {"symbol": "近一月"}),  # 历史查询（可最新） 历史+5-10天更新
    "stock_lhb_jgstatistic_em_3month": (ak.stock_lhb_jgstatistic_em, {"symbol": "近三月"}),
    "stock_lhb_jgstatistic_em_6month": (ak.stock_lhb_jgstatistic_em, {"symbol": "近六月"}),
    "stock_lhb_jgstatistic_em_12month": (ak.stock_lhb_jgstatistic_em, {"symbol": "近一年"}),
    "stock_lhb_yybph_em": (ak.stock_lhb_yybph_em, {"symbol": "近一月"}),  # 月周期
    "stock_lhb_yybph_em_1month": (ak.stock_lhb_yybph_em, {"symbol": "近一月"}),  # 历史查询（可最新） 历史+5-10天更新
    "stock_lhb_yybph_em_3month": (ak.stock_lhb_yybph_em, {"symbol": "近三月"}),
    "stock_lhb_yybph_em_6month": (ak.stock_lhb_yybph_em, {"symbol": "近六月"}),
    "stock_lhb_yybph_em_12month": (ak.stock_lhb_yybph_em, {"symbol": "近一年"}),
    "stock_lhb_traderstatistic_em_1month": (ak.stock_lhb_traderstatistic_em, {"symbol": "近一月"}),  # 较长周期
    "stock_lhb_traderstatistic_em_3month": (ak.stock_lhb_traderstatistic_em, {"symbol": "近三月"}),
    "stock_lhb_traderstatistic_em_6month": (ak.stock_lhb_traderstatistic_em, {"symbol": "近六月"}),
    "stock_lhb_traderstatistic_em_12month": (ak.stock_lhb_traderstatistic_em, {"symbol": "近一年"}),
    # symbol – choice of {"5": 最近 5 天; "10": 最近 10 天; "30": 最近 30 天; "60": 最近 60 天;
    "stock_lhb_ggtj_sina_5d": (ak.stock_lhb_ggtj_sina, {"symbol": "5"}),  # 周期 5天周期全获取一遍
    "stock_lhb_ggtj_sina_10d": (ak.stock_lhb_ggtj_sina, {"symbol": "10"}),
    "stock_lhb_ggtj_sina_30d": (ak.stock_lhb_ggtj_sina, {"symbol": "30"}),
    "stock_lhb_ggtj_sina_60d": (ak.stock_lhb_ggtj_sina, {"symbol": "60"}),
    "stock_lhb_yytj_sina_5d": (ak.stock_lhb_yytj_sina, {"symbol": "5"}),
    "stock_lhb_yytj_sina_10d": (ak.stock_lhb_yytj_sina, {"symbol": "10"}),
    "stock_lhb_yytj_sina_30d": (ak.stock_lhb_yytj_sina, {"symbol": "30"}),
    "stock_lhb_yytj_sina_60d": (ak.stock_lhb_yytj_sina, {"symbol": "60"}),
    "stock_lhb_jgzz_sina_5d": (ak.stock_lhb_jgzz_sina, {"symbol": "5"}),
    "stock_lhb_jgzz_sina_10d": (ak.stock_lhb_jgzz_sina, {"symbol": "10"}),
    "stock_lhb_jgzz_sina_30d": (ak.stock_lhb_jgzz_sina, {"symbol": "30"}),
    "stock_lhb_jgzz_sina_60d": (ak.stock_lhb_jgzz_sina, {"symbol": "60"}),
# 简单输入
    "stock_xgsglb_em": (ak.stock_xgsglb_em, {"symbol": "全部股票"}),  # 全部股票
    "stock_xgsglb_em_north": (ak.stock_xgsglb_em, {"symbol": "北交所"}),  # 北交所
    "stock_xgsglb_em_kcb": (ak.stock_xgsglb_em, {"symbol": "科创板"}),
    "stock_xgsglb_em_hs": (ak.stock_xgsglb_em, {"symbol": "沪市主板"}),
    "stock_xgsglb_em_sz": (ak.stock_xgsglb_em, {"symbol": "深市主板"}),
    "stock_xgsglb_em_cyb": (ak.stock_xgsglb_em, {"symbol": "创业板"}),

    "stock_individual_fund_flow_rank": (ak.stock_individual_fund_flow_rank, {"indicator": "今日"}),
    "stock_individual_fund_flow_rank_3_days": (ak.stock_individual_fund_flow_rank, {"indicator": "3日"}),
    "stock_individual_fund_flow_rank_5d": (ak.stock_individual_fund_flow_rank, {"indicator": "5日"}),
    "stock_individual_fund_flow_rank_10d": (ak.stock_individual_fund_flow_rank, {"indicator": "10日"}),

    # :param symbol: choice of {“即时”, "3日排行", "5日排行", "10日排行", "20日排行"}
    "stock_fund_flow_individual_now": (
        ak.stock_fund_flow_individual, {"symbol": "即时"}),
    "stock_fund_flow_individual_3d": (
        ak.stock_fund_flow_individual, {"symbol": "3日排行"}),
    "stock_fund_flow_individual_5d": (
        ak.stock_fund_flow_individual, {"symbol": "5日排行"}),
    "stock_fund_flow_individual_10d": (
        ak.stock_fund_flow_individual, {"symbol": "10日排行"}),
    "stock_fund_flow_individual_20d": (
        ak.stock_fund_flow_individual, {"symbol": "20日排行"}),
    "stock_fund_flow_concept_now": (
        ak.stock_fund_flow_concept, {"symbol": "即时"}),
    "stock_fund_flow_concept_3d": (
        ak.stock_fund_flow_concept, {"symbol": "3日排行"}),
    "stock_fund_flow_concept_5d": (
        ak.stock_fund_flow_concept, {"symbol": "5日排行"}),
    "stock_fund_flow_concept_10d": (
        ak.stock_fund_flow_concept, {"symbol": "10日排行"}),
    "stock_fund_flow_concept_20d": (
        ak.stock_fund_flow_concept, {"symbol": "20日排行"}),
    "stock_fund_flow_industry_now": (
        ak.stock_fund_flow_industry, {"symbol": "即时"}),
    "stock_fund_flow_industry_3d": (
        ak.stock_fund_flow_industry, {"symbol": "3日排行"}),
    "stock_fund_flow_industry_5d": (
        ak.stock_fund_flow_industry, {"symbol": "5日排行"}),
    "stock_fund_flow_industry_10d": (
        ak.stock_fund_flow_industry, {"symbol": "10日排行"}),
    "stock_fund_flow_industry_20d": (
        ak.stock_fund_flow_industry, {"symbol": "20日排行"}),


    # Params:
    # indicator – choice of {"今日", "5日", "10日"}
    # sector_type – choice of {"行业资金流", "概念资金流", "地域资金流"}
    "stock_sector_fund_flow_rank_today_industry": (
    ak.stock_sector_fund_flow_rank, {"indicator": "今日", "sector_type": "行业资金流"}),
    "stock_sector_fund_flow_rank_5d_industry": (
        ak.stock_sector_fund_flow_rank, {"indicator": "5日", "sector_type": "行业资金流"}),
    "stock_sector_fund_flow_rank_10d_industry": (
        ak.stock_sector_fund_flow_rank, {"indicator": "10日", "sector_type": "行业资金流"}),
    "stock_sector_fund_flow_rank_today_concept": (
        ak.stock_sector_fund_flow_rank, {"indicator": "今日", "sector_type": "概念资金流"}),
    "stock_sector_fund_flow_rank_5d_concept": (
        ak.stock_sector_fund_flow_rank, {"indicator": "5日", "sector_type": "概念资金流"}),
    "stock_sector_fund_flow_rank_10d_concept": (
        ak.stock_sector_fund_flow_rank, {"indicator": "10日", "sector_type": "概念资金流"}),
    "stock_sector_fund_flow_rank_today_region": (
        ak.stock_sector_fund_flow_rank, {"indicator": "今日", "sector_type": "地域资金流"}),
    "stock_sector_fund_flow_rank_5d_region": (
        ak.stock_sector_fund_flow_rank, {"indicator": "5日", "sector_type": "地域资金流"}),
    "stock_sector_fund_flow_rank_10d_region": (
        ak.stock_sector_fund_flow_rank, {"indicator": "10日", "sector_type": "地域资金流"}),

    # Params:
    # symbol – 行业类型; choice of {"证监会行业分类标准", "巨潮行业分类标准", "申银万国行业分类标准", "新财富行业分类标准", "国资委行业分类标准", "巨潮产业细分标准", "天相行业分类标准", "全球行业分类标准"
    "stock_industry_category_cninfo_cs": (
        ak.stock_industry_category_cninfo, {"symbol": "证监会行业分类标准"}),
    "stock_industry_category_cninfo_jc": (
        ak.stock_industry_category_cninfo, {"symbol": "巨潮行业分类标准"}),
    "stock_industry_category_cninfo_sy": (
        ak.stock_industry_category_cninfo, {"symbol": "申银万国行业分类标准"}),
    "stock_industry_category_cninfo_xf": (
        ak.stock_industry_category_cninfo, {"symbol": "新财富行业分类标准"}),
    "stock_industry_category_cninfo_gzw": (
        ak.stock_industry_category_cninfo, {"symbol": "国资委行业分类标准"}),
    "stock_industry_category_cninfo_jc_sub": (
        ak.stock_industry_category_cninfo, {"symbol": "巨潮产业细分标准"}),
    "stock_industry_category_cninfo_tx": (
        ak.stock_industry_category_cninfo, {"symbol": "天相行业分类标准"}),
    "stock_industry_category_cninfo_global": (
        ak.stock_industry_category_cninfo, {"symbol": "全球行业分类标准"}),

    # Params:
    # symbol – choice of {"全部", "股东增持", "股东减持"}
    "stock_ggcg_em_all": (
        ak.stock_ggcg_em, {"symbol": "全部"}),
    "stock_ggcg_em_increase": (
        ak.stock_ggcg_em, {"symbol": "股东增持"}),
    "stock_ggcg_em_decrease": (
        ak.stock_ggcg_em, {"symbol": "股东减持"}),
    # Params:
    # symbol – 全部股票; choice of {"全部股票", "沪深A股", "沪市A股", "科创板", "深市A股", "创业板", "沪市B股", "深市B股"}
    "stock_main_fund_flow_all": (
    ak.stock_main_fund_flow, {"symbol": "全部股票"}),
    "stock_main_fund_flow_shsz": (
        ak.stock_main_fund_flow, {"symbol": "沪深A股"}),
    "stock_main_fund_flow_sh": (
        ak.stock_main_fund_flow, {"symbol": "沪市A股"}),
    "stock_main_fund_flow_science_board": (
        ak.stock_main_fund_flow, {"symbol": "科创板"}),
    "stock_main_fund_flow_sz": (
        ak.stock_main_fund_flow, {"symbol": "深市A股"}),
    "stock_main_fund_flow_gem": (
        ak.stock_main_fund_flow, {"symbol": "创业板"}),
    "stock_main_fund_flow_sh_b": (
        ak.stock_main_fund_flow, {"symbol": "沪市B股"}),
    "stock_main_fund_flow_sz_b": (
        ak.stock_main_fund_flow, {"symbol": "深市B股"}),
    # 日度
    # 分时
    "stock_hsgt_fund_min_em_north": (ak.stock_hsgt_fund_min_em, {"symbol": "北向资金"}),  # 沪深港通分时数据
    "stock_hsgt_fund_min_em_south": (ak.stock_hsgt_fund_min_em, {"symbol": "南向资金"}),  # 沪深港通分时数据
}
api_void_monthly={
    "stock_lh_yyb_most": (ak.stock_lh_yyb_most, {}), # 每交易日
    "stock_a_ttm_lyr": (ak.stock_a_ttm_lyr, {}),  # 月度，最新的实时
    "stock_a_all_pb": (ak.stock_a_all_pb, {}),  # 近三个月
    "stock_new_gh_cninfo": (ak.stock_new_gh_cninfo, {}),  # 历史记录，最新，周期获取或指定时间更新
    "stock_new_ipo_cninfo": (ak.stock_new_ipo_cninfo, {}),  # 季度
    "stock_industry_clf_hist_sw": (ak.stock_industry_clf_hist_sw, {}),  # 历史记录
}


def call_func_traversal(basic_name, begin_date, end_date, id, interrupt_file, processed_interfaces,
                    base_path='./stock_data/stock_relative'):
    error_file = os.path.join(base_path, f"{basic_name}_error_reports_{id}.json")
    error_reports = load_json(error_file)
    if not isinstance(error_reports, list):
        error_reports = []
    i = 0
    # 接口调用的相关定义
    api_range = {
        # 输入直接是日期区间
        "stock_lhb_detail_em": (ak.stock_lhb_detail_em, {"start_date": f"{begin_date}", "end_date": f"{end_date}"}),
        "stock_lhb_jgmmtj_em": (ak.stock_lhb_jgmmtj_em, {"start_date": f"{begin_date}", "end_date": f"{end_date}"}),
        # 历史查询
        "stock_lhb_hyyyb_em": (ak.stock_lhb_hyyyb_em, {"start_date": f"{begin_date}", "end_date": f"{end_date}"}),
        # 输入要靠自定义函数转换，return的最终结果得是dataframe 吗？ 先这样写，要改另说。标准化接口
        "stock_institute_hold": (stock_institute_hold, {"start_date": f"{begin_date}", "end_date": f"{end_date}"}),
    }

    total_interfaces = len(api_range)
    for key, (func, params) in api_range.items():
        if key in processed_interfaces:
            # print(f"接口 {key} 在 {id}已处理，跳过 ")
            continue
        i += 1
        try:
            df = func(**params)
            print(f"successful getting {key}")
            print(df)
            save_to_json_v2(df, f"{base_path}/{key}_{fetch_date}.json")
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

def stock_institute_hold(begin_date, end_date):
    all_data = pd.DataFrame()
    quarters = generate_quarters(begin_date,end_date)

    for item in quarters:
        df = ak.stock_rank_forecast_cninfo(date=item)  # 机构的，历史记录，输入有点特殊
        if not df.empty:
            all_data = pd.concat([all_data, df], ignore_index=True)
        else:
            print(f"Warning: Interface ak.stock_rank_forecast_cninfo: datetime{item} return empty dataframes.")
    return all_data


def stock_rank_forecast_cninfo(begin_date, end_date):
    all_data = pd.DataFrame()
    quarters = generate_dates(begin_date,end_date)

    for item in quarters:
        df = ak.stock_institute_hold(symbol=item)
        if not df.empty:
            all_data = pd.concat([all_data, df], ignore_index=True)
        else:
            print(f"Warning: Interface ak.stock_institute_hold: datetime{item} return empty dataframes.")
    return all_data

def stock_industry_pe_ratio_cninfo_gz(begin_date, end_date):
    all_data = pd.DataFrame()
    quarters = generate_dates(begin_date,end_date)

    for item in quarters:
        df = ak.stock_industry_pe_ratio_cninfo(symbol="国证行业分类", date=item)
        if not df.empty:
            all_data = pd.concat([all_data, df], ignore_index=True)
        else:
            print(f"Warning: Interface ak.stock_industry_pe_ratio_cninfo: datetime{item} return empty dataframes.")
    return all_data

def stock_industry_pe_ratio_cninfo_zjh(begin_date, end_date):
    all_data = pd.DataFrame()
    quarters = generate_dates(begin_date,end_date)

    for item in quarters:
        df = ak.stock_industry_pe_ratio_cninfo(symbol="证监会行业分类", date=item)
        if not df.empty:
            all_data = pd.concat([all_data, df], ignore_index=True)
        else:
            print(f"Warning: Interface ak.stock_industry_pe_ratio_cninfo: datetime{item} return empty dataframes.")
    return all_data

def stock_report_fund_hold_jj(begin_date, end_date):
    all_data = pd.DataFrame()
    quarters = generate_report_dates(begin_date,end_date)

    for item in quarters:
        df = ak.stock_report_fund_hold(symbol="基金持仓",date=item)
        if not df.empty:
            all_data = pd.concat([all_data, df], ignore_index=True)
        else:
            print(f"Warning: Interface ak.stock_report_fund_hold: datetime{item} return empty dataframes.")
    return all_data

def stock_report_fund_hold_QFII(begin_date, end_date):
    all_data = pd.DataFrame()
    quarters = generate_report_dates(begin_date, end_date)

    for item in quarters:
        df = ak.stock_report_fund_hold(symbol="QFII持仓", date=item)
        if not df.empty:
            all_data = pd.concat([all_data, df], ignore_index=True)
        else:
            print(f"Warning: Interface ak.stock_report_fund_hold: date {item} returned empty dataframes.")
    return all_data


def stock_report_fund_hold_sb(begin_date, end_date):
    all_data = pd.DataFrame()
    quarters = generate_report_dates(begin_date, end_date)

    for item in quarters:
        df = ak.stock_report_fund_hold(symbol="社保持仓", date=item)
        if not df.empty:
            all_data = pd.concat([all_data, df], ignore_index=True)
        else:
            print(f"Warning: Interface ak.stock_report_fund_hold: date {item} returned empty dataframes.")
    return all_data

def stock_report_fund_hold_js(begin_date, end_date):
    all_data = pd.DataFrame()
    quarters = generate_report_dates(begin_date, end_date)

    for item in quarters:
        df = ak.stock_report_fund_hold(symbol="券商持仓", date=item)
        if not df.empty:
            all_data = pd.concat([all_data, df], ignore_index=True)
        else:
            print(f"Warning: Interface ak.stock_report_fund_hold: date {item} returned empty dataframes.")
    return all_data

def stock_report_fund_hold_bx(begin_date, end_date):
    all_data = pd.DataFrame()
    quarters = generate_report_dates(begin_date, end_date)

    for item in quarters:
        df = ak.stock_report_fund_hold(symbol="保险持仓", date=item)
        if not df.empty:
            all_data = pd.concat([all_data, df], ignore_index=True)
        else:
            print(f"Warning: Interface ak.stock_report_fund_hold: date {item} returned empty dataframes.")
    return all_data

def stock_report_fund_hold_xt(begin_date, end_date):
    all_data = pd.DataFrame()
    quarters = generate_report_dates(begin_date, end_date)

    for item in quarters:
        df = ak.stock_report_fund_hold(symbol="信托持仓", date=item)
        if not df.empty:
            all_data = pd.concat([all_data, df], ignore_index=True)
        else:
            print(f"Warning: Interface ak.stock_report_fund_hold: date {item} returned empty dataframes.")
    return all_data

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


def call_date_range(basic_name, begin_date, end_date, id, interrupt_file, processed_interfaces, base_path='./stock_data/stock_relative'):
    error_file = os.path.join(base_path, f"{basic_name}_error_reports_{id}.json")
    error_reports = load_json(error_file)
    if not isinstance(error_reports, list):
        error_reports = []
    i = 0
    # 接口调用的相关定义
    api_range={
        # 输入直接是日期区间
        "stock_lhb_detail_em": (ak.stock_lhb_detail_em, {"start_date": f"{begin_date}", "end_date": f"{end_date}"}),
        "stock_lhb_jgmmtj_em": (ak.stock_lhb_jgmmtj_em, {"start_date": f"{begin_date}", "end_date": f"{end_date}"}), #历史查询
        "stock_lhb_hyyyb_em": (ak.stock_lhb_hyyyb_em, {"start_date": f"{begin_date}", "end_date": f"{end_date}"}),
        # 输入要靠自定义函数转换，return的最终结果得是dataframe 吗？ 先这样写，要改另说。标准化接口
        "stock_institute_hold": (stock_institute_hold,{"start_date": f"{begin_date}", "end_date": f"{end_date}"}),  # 机构的，历史记录，输入有点特殊
        "stock_rank_forecast_cninfo": (stock_institute_hold,{"start_date": f"{begin_date}", "end_date": f"{end_date}"}), # 历史记录，输入日期查
        "stock_industry_pe_ratio_cninfo_gz": (stock_industry_pe_ratio_cninfo_gz,{"start_date": f"{begin_date}", "end_date": f"{end_date}"}), # 历史记录，输入日期查
        "stock_industry_pe_ratio_cninfo_zjh": (stock_industry_pe_ratio_cninfo_zjh,{"start_date": f"{begin_date}", "end_date": f"{end_date}"}),
        # symbol – choice of {"基金持仓", "QFII持仓", "社保持仓", "券商持仓", "保险持仓", "信托持仓"}
        # date – 财报发布日期, xxxx-03-31, xxxx-06-30, xxxx-09-30, xxxx-12-31
        "stock_report_fund_hold_jj": (stock_report_fund_hold_jj,{"start_date": f"{begin_date}", "end_date": f"{end_date}"}), # 季度日期查询，日期后获取
        "stock_report_fund_hold_QFII": (stock_report_fund_hold_QFII, {"start_date": f"{begin_date}", "end_date": f"{end_date}"}),
        "stock_report_fund_hold_sb": (stock_report_fund_hold_sb, {"start_date": f"{begin_date}", "end_date": f"{end_date}"}),
        "stock_report_fund_hold_js": (stock_report_fund_hold_js, {"start_date": f"{begin_date}", "end_date": f"{end_date}"}),
        "stock_report_fund_hold_bx": (stock_report_fund_hold_bx, {"start_date": f"{begin_date}", "end_date": f"{end_date}"}),
        "stock_report_fund_hold_xt": (stock_report_fund_hold_xt, {"start_date": f"{begin_date}", "end_date": f"{end_date}"}),
        "stock_lhb_detail_daily_sina": (stock_lhb_detail_daily_sina,{"start_date": f"{begin_date}", "end_date": f"{end_date}"}), # 每日

        # "stock_jgdy_tj_em": (ak.stock_jgdy_tj_em, {"date": "20180928"}),  # 机构调研统计接口
        # "stock_jgdy_detail_em": (ak.stock_jgdy_detail_em, {"date": "20180928"}),  # 机构调研详细接口
        # "stock_gpzy_pledge_ratio_em": (ak.stock_gpzy_pledge_ratio_em, {"date": "20231020"}),  # 上市公司质押比例接口
        # "stock_sy_yq_em": (ak.stock_sy_yq_em, {"date": "20221231"}),  # 商誉减值预期明细
        # "stock_sy_jz_em": (ak.stock_sy_jz_em, {"date": "20230331"}),  # 个股商誉减值明细
        # "stock_sy_em": (ak.stock_sy_em, {"date": "20230331"}),  # 个股商誉明细  # 参考网站指定的数据日期
        # "stock_sy_hy_em": (ak.stock_sy_hy_em, {"date": "20230331"}),  # 行业商誉数据  # 参考网站指定的数据日期
        # "stock_tfp_em": (ak.stock_tfp_em, {"date": "20240426"}),  # 停复牌信息
        # "stock_zcfz_em": (ak.stock_zcfz_em, {"date": "20220331"}),  # 资产负债表
        #
        # "stock_lrb_em": (ak.stock_lrb_em, {"date": "20220331"}),  # 利润表
        # "stock_xjll_em": (ak.stock_xjll_em, {"date": "20200331"}),  # 现金流量表
        # "stock_fhps_em": (ak.stock_fhps_em, {"date": "20231231"}),  # 分红配送  # date是分红配送报告期
        #
        # "news_trade_notify_suspend_baidu": (ak.news_trade_notify_suspend_baidu, {"date": "20220513"}),  # 停复牌
        # "news_trade_notify_dividend_baidu": (ak.news_trade_notify_dividend_baidu, {"date": "20220916"}),  # 分红派息
        # "news_report_time_baidu": (ak.news_report_time_baidu, {"date": "20220514"}),  # 财报发行
        #
        #
        # "stock_yjkb_em": (ak.stock_yjkb_em, {"date": "20200331"}),  # 业绩快报 财报日期
        # "stock_yjyg_em": (ak.stock_yjyg_em, {"date": "20190331"}),  # 业绩预告 财报日期
        # "stock_yysj_em": (ak.stock_yysj_em, {"symbol": "沪深A股", "date": "20211231"}),  # 预约披露时间
        #
        # "stock_report_disclosure": (ak.stock_report_disclosure, {"market": "沪深京", "period": "2022年报"}),  # 预约披露
        #
        # "stock_analyst_rank_em": (ak.stock_analyst_rank_em, {"year": "2022"}),  # 分析师指数排行
    }

    total_interfaces = len(api_range)
    for key, (func, params) in api_range.items():
        if key in processed_interfaces:
            # print(f"接口 {key} 在 {id}已处理，跳过 ")
            continue
        i += 1
        try:
            df = func(**params)
            print(f"successful getting {key}")
            print(df)
            save_to_json_v2(df, f"{base_path}/{key}_{fetch_date}.json")
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



def call_all_functions(basic_name, api_func_dict, report_date, interrupt_file, processed_interfaces, base_path='./stock_data/stock_relative'):
    error_file = os.path.join(base_path, f"{basic_name}_error_reports_{report_date}.json")
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
            print(df)
            save_to_json_v2(df, f"{base_path}/{key}_{fetch_date}.json")
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


def calling_func():
    basic_name = "stock_relative_fetch"
    fetch_date = datetime.now().strftime("%Y%m%d")
    base_path = './stock_data/stock_relative'
    os.makedirs(base_path,exist_ok=True)

    interrupt_file = os.path.join(base_path, f'{basic_name}_interrupt_{fetch_date}.json')
    interrupt_data = load_json(interrupt_file)
    if not isinstance(interrupt_data, dict):
        interrupt_data = {}
    processed_interfaces = set(interrupt_data.get('processed_interfaces', []))

    call_all_functions(basic_name, api_functions, fetch_date, interrupt_file, processed_interfaces, base_path)  # 自定义周期
    call_all_functions(basic_name, api_void, fetch_date, interrupt_file, processed_interfaces, base_path)  # 自定义周期
    call_all_functions(basic_name, api_void_daily, fetch_date, interrupt_file, processed_interfaces, base_path)  # 5天一次
    call_all_functions(basic_name, api_void_monthly, fetch_date, interrupt_file, processed_interfaces, base_path)  # 月度的自定义周期
    call_date_range(basic_name, "20240701", "20240716", fetch_date, interrupt_file, processed_interfaces, base_path)




