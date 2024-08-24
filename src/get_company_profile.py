import os
import json
import pandas as pd
import akshare as ak
from datetime import date, datetime, timedelta
from basic_func import save_to_json_v2
from tqdm import tqdm
from basic_func import find_latest_file_v2
class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()
        return super(DateEncoder, self).default(obj)


def save_to_json(data, path):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4, cls=DateEncoder)


def load_json(path):
    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}


def get_yesterday():
    return (datetime.now() - timedelta(1)).strftime('%Y%m%d')


def get_company_basic_profile(stock_dict, base_path, processed_stocks, flag, report_date, interrupt_file):
    """
    获取每日报表
    :param stock_dict: 已生成的的深A或沪A股字典
    :param base_path: 每日报表生成的目标路径,为根路径下的的company_data文件夹
    :param processed_stocks: 中断处理使用，记录已处理的公司
    :param flag: 1表示深A股字典，0表示沪A股字典；内部需要使用不同接口
    :param report_date: 指定的日期
    :return: 无返回值，直接写入文件并存储
    """
    frequency = 300
    loading_prior = True  # 正常情况下，不会超频的接口会采取直接调用的形式

    company_basic_profiles_file = os.path.join(base_path, f"company_basic_profiles_{report_date}.json")
    error_reports_file = os.path.join(base_path, f"error_reports_{report_date}.json")

    company_basic_profiles = load_json(company_basic_profiles_file)
    error_reports = load_json(error_reports_file)

    if not isinstance(company_basic_profiles, list):
        company_basic_profiles = []

    if not isinstance(error_reports, list):
        error_reports = []

    if flag:
        market = "深A股"
        # region = "sz"
    else:
        market = "沪A股"
        # region = "sh"

    report_date_str = report_date.replace("-", "").replace(":", "").replace(" ", "")
    today_str = date.today().strftime("%Y%m%d")

    total_stocks = len(stock_dict)

    # for i, stock in tqdm(enumerate(stock_dict), total=total_stocks, desc=f"Processing stock from {region}"):
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
            company_name_safe = company_name.replace("*", "")  # 替换 * 字符

            # 写入的文件路径
            company_basic_profile_file = os.path.join(base_path, market, company_name_safe,
                                                      f"{company_name_safe}_company_basic_profile_{report_date}.json")
            os.makedirs(os.path.dirname(company_basic_profile_file), exist_ok=True)
            company_file = os.path.join(base_path, market, company_name_safe)

            # 获取总股本
            stock_individual_info_em_path = os.path.join(company_file, f"stock_individual_info_em_{today_str}.json")
            stock_individual_info_em_df = pd.DataFrame()
            if loading_prior:
                stock_individual_info_path = find_latest_file_v2(company_file, f"stock_individual_info_em_", after_date=report_date_str)
                if stock_individual_info_path:
                    stock_individual_info_em_df = pd.DataFrame(load_json(stock_individual_info_path))

            if loading_prior and not stock_individual_info_em_df.empty:
                total_share_capital = \
                    stock_individual_info_em_df[stock_individual_info_em_df['item'] == '总股本']['value'].values[0]
            else:
                stock_individual_info_em_df = ak.stock_individual_info_em(symbol=stock_code)
                if stock_individual_info_em_df.empty:
                    print(f"无法获取公司 {stock_name} 代码 {stock_code} 的个股信息，对应接口：ak.stock_individual_info_em")
                    error_reports.append({"stock_name": stock_name, "stock_code": stock_code, "flag": flag,
                                          "error": "无法获取ak.stock_individual_info_em"})
                    stock_zh_a_gdhs_detail_em_df = ak.stock_zh_a_gdhs_detail_em(symbol=stock_code)
                    if stock_zh_a_gdhs_detail_em_df.empty:
                        print(
                            f"无法获取公司 {stock_name} 代码 {stock_code} 的总股本信息，对应接口：ak.stock_zh_a_gdhs_detail_em")
                        continue
                    else:
                        total_share_capital = \
                            stock_zh_a_gdhs_detail_em_df[stock_zh_a_gdhs_detail_em_df['item'] == '总股本']['value'].values[0]
                else:
                    total_share_capital = \
                    stock_individual_info_em_df[stock_individual_info_em_df['item'] == '总股本']['value'].values[0]
                    save_to_json_v2(stock_individual_info_em_df, stock_individual_info_em_path)

            # 获取发行量、发行价格、发行日期
            stock_ipo_summary_cninfo_filepath = os.path.join(company_file, f"stock_ipo_summary_cninfo_{today_str}.json")
            stock_ipo_summary_cninfo_df = pd.DataFrame()
            if loading_prior:
                stock_ipo_summary_cninfo_path = find_latest_file_v2(company_file, f"stock_ipo_summary_cninfo_", after_date=report_date_str)
                if stock_ipo_summary_cninfo_path:
                    stock_ipo_summary_cninfo_df = pd.DataFrame(load_json(stock_ipo_summary_cninfo_path), index=[0])

            if loading_prior and not stock_ipo_summary_cninfo_df.empty:
                issue_size = stock_ipo_summary_cninfo_df.at[0, '总发行数量']
                issue_price = stock_ipo_summary_cninfo_df.at[0, '发行价格']
                issue_date = stock_ipo_summary_cninfo_df.at[0, '上网发行日期']
                # issue_date = pd.to_datetime(stock_ipo_summary_cninfo_df.at[0, '上网发行日期'], unit='ms').strftime('%Y-%m-%d')
            else:
                stock_ipo_summary_cninfo_df = ak.stock_ipo_summary_cninfo(symbol=stock_code)  # 高频调用ban
                if stock_ipo_summary_cninfo_df.empty:
                    print(f"无法获取公司 {stock_name} 代码 {stock_code} 的上市相关-巨潮资讯，对应接口：ak.stock_ipo_summary_cninfo")
                    error_reports.append({"stock_name": stock_name, "stock_code": stock_code, "flag": flag})
                    continue
                for col in stock_ipo_summary_cninfo_df.columns:
                    if pd.api.types.is_datetime64_any_dtype(stock_ipo_summary_cninfo_df[col]):
                        stock_ipo_summary_cninfo_df[col] = stock_ipo_summary_cninfo_df[col].apply(
                            lambda x: x.isoformat() if pd.notnull(x) else None)
                    elif pd.api.types.is_object_dtype(stock_ipo_summary_cninfo_df[col]):
                        stock_ipo_summary_cninfo_df[col] = stock_ipo_summary_cninfo_df[col].astype(str)

                issue_size = stock_ipo_summary_cninfo_df.at[0, '总发行数量']
                issue_price = stock_ipo_summary_cninfo_df.at[0, '发行价格']
                issue_date = stock_ipo_summary_cninfo_df.at[0, '上网发行日期']

                save_to_json_v2(stock_ipo_summary_cninfo_df, stock_ipo_summary_cninfo_filepath)


            # 尝试读取已有公司基本信息
            company_profile_path = find_latest_file_v2(company_file, f"{company_name_safe}_profile_")

            if company_profile_path:
                # print(latest_file_path_1)
                profile_df = pd.DataFrame(load_json(company_profile_path), index=[0])
                company_fullname = profile_df.at[0, '公司名称']
                main_business = profile_df.at[0, '主营业务']
                business_scope = profile_df.at[0, '经营范围']
                company_profile = profile_df.at[0, '机构简介']
                registered_capital = profile_df.at[0, '注册资金'] # 注册资本不是注册资金
                establishment_date = profile_df.at[0, '上市日期']
                region = profile_df.at[0, '经营范围'] # ?
                company_website = profile_df.at[0, '官方网站']
                company_email = profile_df.at[0, '电子邮箱']
            else:
                # 错误处理
                profile_df = ak.stock_profile_cninfo(symbol=stock_code)
                save_to_json_v2(profile_df, os.path.join(company_file, f"{company_name_safe}_profile_{today_str}.json"))
                company_fullname = profile_df.at[0, '公司名称']
                main_business = profile_df.at[0, '主营业务']
                business_scope = profile_df.at[0, '经营范围']
                company_profile = profile_df.at[0, '机构简介']
                registered_capital = profile_df.at[0, '注册资金'] # 注册资本不是注册资金
                establishment_date = profile_df.at[0, '上市日期']
                region = profile_df.at[0, '经营范围'] # ?
                company_website = profile_df.at[0, '官方网站']
                company_email = profile_df.at[0, '电子邮箱']

            company_basic_profile = {
                "公司名称": company_fullname,
                "A股简称": stock_name,
                "所属区域": region,
                "股票代码": stock_code,
                "主营业务": main_business,
                "经营范围": business_scope,
                "公司简介": company_profile,
                "总股本": total_share_capital,
                "发行量": issue_size,
                "发行价格": issue_price,
                "发行日期": issue_date,
                "注册资本": registered_capital,
                "成立日期": establishment_date,
                # "是否注册制": registration_system,  # 暂时获取不了
                "公司网址": company_website,
                "公司邮箱": company_email
            }

            company_basic_profiles.append(company_basic_profile)

            # 将每日报表保存为JSON文件
            save_to_json(company_basic_profile, company_basic_profile_file)

            # 记录已处理的股票
            processed_stocks.add(stock_code)

            # 定期保存中间结果和中断点
            if (i + 1) % 10 == 0 or i == total_stocks - 1:
                save_to_json(company_basic_profiles, company_basic_profiles_file)
                save_to_json({"processed_stocks": list(processed_stocks)}, interrupt_file)
                save_to_json(error_reports, error_reports_file)
                if (i + 1) % frequency == 0:
                    print(f"Progress: {i + 1}/{total_stocks} stocks processed.")

        except Exception as e:
            print(f"Error processing stock {stock_code}: {e}")
            error_reports.append({"stock_name": stock_name, "stock_code": stock_code, "flag": flag})
            if (i + 1) % 10 == 0 or i == total_stocks - 1:
                save_to_json(company_basic_profiles, company_basic_profiles_file)
                save_to_json({"processed_stocks": list(processed_stocks)}, interrupt_file)
                save_to_json(error_reports, error_reports_file)
                if (i + 1) % frequency == 0:
                    print(f"Progress: {i + 1}/{total_stocks} stocks processed.")
            continue

        # 保存最终结果
        save_to_json(company_basic_profiles, company_basic_profiles_file)
        save_to_json({"processed_stocks": list(processed_stocks)}, interrupt_file)
        save_to_json(error_reports, error_reports_file)


def get_company_basic_profiles(base_path='./stock_data', report_date=get_yesterday()):
    """
    在收盘后获取公司的每日报告，可以在开闭市的总市值与市盈率错误的情况下，读取非当日报告；但此时读取历史数据失败的错误不可接受
    :param base_path: 基本路径，默认为'./stock_data/company_data'。同样涉及到已有文件结构。
    :param report_date 指定每日报告的日期，YYYYMMDD的str，默认是昨天；若非当日闭市隔次开市前读取，市盈率一定是错的（由实时数据读取得到）
    :return: 直接将每日报告写入公司文件夹。
    """
    print("now executing function get_company_basic_profiles")
    company_base_path = os.path.join(base_path, "company_data")

    # 读取沪A股和深A股的数据
    stock_sh_a_spot_em_df = ak.stock_sh_a_spot_em()
    stock_sz_a_spot_em_df = ak.stock_sz_a_spot_em()

    # 提取沪A股的编号、名称和代码
    sh_a_stocks = stock_sh_a_spot_em_df[['序号', '名称', '代码']].drop_duplicates().to_dict(orient='records')
    # 提取深A股的编号、名称和代码
    sz_a_stocks = stock_sz_a_spot_em_df[['序号', '名称', '代码']].drop_duplicates().to_dict(orient='records')
    #
    save_to_json(sh_a_stocks, os.path.join(company_base_path, "sh_a_stocks.json"))
    save_to_json(sz_a_stocks, os.path.join(company_base_path, "sz_a_stocks.json"))
    sh_a_stocks = load_json(os.path.join(company_base_path, "sh_a_stocks.json"))
    sz_a_stocks = load_json(os.path.join(company_base_path, "sz_a_stocks.json"))

    # # 指定日期
    # report_date = get_yesterday()

    # 加载中断点记录
    interrupt_file = os.path.join(company_base_path, f'company_basic_profiles_interrupt_{report_date}.json')
    interrupt_data = load_json(interrupt_file)
    # Ensure interrupt_data is a dictionary
    if not isinstance(interrupt_data, dict):
        interrupt_data = {}
    processed_stocks = set(interrupt_data.get('processed_stocks', []))

    # 生成沪A股和深A股的每日报表
    get_company_basic_profile(sh_a_stocks, company_base_path, processed_stocks, 0, report_date, interrupt_file)
    get_company_basic_profile(sz_a_stocks, company_base_path, processed_stocks, 1, report_date, interrupt_file)
    print("Successfully executing function get_company_basic_profiles")

if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(current_dir)
    base_path = os.path.join(parent_dir, 'data', 'stock_data')  # 数据文件的根目录
    get_company_basic_profiles(base_path=base_path, report_date=datetime.now().strftime("%Y%m%d"))
