import os
import json
import pandas as pd
import akshare as ak
from datetime import date, datetime, timedelta


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

def find_latest_file(base_directory, name_prefix, before_date=None):
    """
    寻找并返回前缀符合，日期最新的文件路径
    :param base_directory: 寻找文件的路径
    :param name_prefix: 文件的前缀
    :param before_date: 可选参数，形式为YYYYMMDD，输入后返回该日期之前（不含该日期）的日期最新的文件
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


def get_average_elements(company_file, stock_code):
    """
    获取公司相关字段中的三个平均值
    :param company_file: 已经计算出的公司文件夹路径
    :param stock_code: 股票代码，纯数字如“000063”
    :return: 近三年净利润平均值: net_profit_last_three_years ;近三年营业收入增长平均值: revenue_growth_last_three_years; 近三年资产负债率平均值: asset_liability_ratio_last_three_years
    """
    # 定义一个函数来转换单位为数字
    def convert_to_number(value):
        if '万亿' in value:
            return float(value.replace('万亿', '')) * 1e12
        elif '亿' in value:
            return float(value.replace('亿', '')) * 1e8
        elif '万' in value:
            return float(value.replace('万', '')) * 1e4
        else:
            return float(value)


    # 获取股票代码对应的年度财务摘要信息
    stock_financial_abstract_ths_df = ak.stock_financial_abstract_ths(symbol=stock_code, indicator="按年度")

    # 选择需要的列，并将报告期列重命名为年份
    selected_columns = stock_financial_abstract_ths_df[['报告期', '净利润', '营业总收入', '资产负债率']].copy()
    selected_columns.rename(columns={'报告期': '年份'}, inplace=True)

    # 将年份转换为整数类型
    selected_columns.loc[:, '年份'] = selected_columns['年份'].astype(int)
    # 应用函数转换净利润和营业总收入列
    selected_columns['净利润'] = selected_columns['净利润'].apply(convert_to_number)
    selected_columns['营业总收入'] = selected_columns['营业总收入'].apply(convert_to_number)
    selected_columns['资产负债率'] = selected_columns['资产负债率'].str.replace('%', '').astype(float)

    # 过滤最近三年的数据
    recent_three_years = selected_columns[selected_columns['年份'] >= selected_columns['年份'].max() - 2]

    # 计算最近三年的净利润平均值
    average_net_profit = recent_three_years['净利润'].mean()

    # 计算最近三年的营业总收入增长值的平均值
    average_revenue_growth = recent_three_years['营业总收入'].mean()

    # 计算最近三年的资产负债率平均值
    average_asset_liability_ratio = recent_three_years['资产负债率'].mean()

    # 定义一个函数将数字还原为带单位的字符串
    def convert_to_unit(value):
        if value >= 1e12:
            return f"{value / 1e12:.2f}万亿"
        elif value >= 1e8:
            return f"{value / 1e8:.2f}亿"
        elif value >= 1e4:
            return f"{value / 1e4:.2f}万"
        else:
            return str(value)

    # 创建一个 DataFrame 存储这些平均值
    profile_df = pd.DataFrame({
        '近三年净利润': [convert_to_unit(average_net_profit)],
        '近三年营业收入增长': [convert_to_unit(average_revenue_growth)],
        '近三年资产负债率': [f"{average_asset_liability_ratio:.2f}%"]
    })

    net_profit_last_three_years = profile_df.at[0, '近三年净利润']
    revenue_growth_last_three_years = profile_df.at[0, '近三年营业收入增长']
    asset_liability_ratio_last_three_years = profile_df.at[0, '近三年资产负债率']

    return net_profit_last_three_years,revenue_growth_last_three_years,asset_liability_ratio_last_three_years



def get_company_relative_profile(stock_dict, base_path, processed_stocks, flag, report_date, stock_sh_a_spot_em_df,
                                 stock_sz_a_spot_em_df, interrupt_file):
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

    company_relative_profiles_file = os.path.join(base_path, f"company_relative_profiles_{report_date}.json")
    error_reports_file = os.path.join(base_path, f"error_reports_{report_date}.json")

    company_relative_profiles = load_json(company_relative_profiles_file)
    error_reports = load_json(error_reports_file)

    if not isinstance(company_relative_profiles, list):
        company_relative_profiles = []

    if not isinstance(error_reports, list):
        error_reports = []

    total_stocks = len(stock_dict)
    for i, stock in enumerate(stock_dict):
        stock_code = stock['代码']
        stock_name = stock['名称']

        # 跳过已处理的股票
        if stock_code in processed_stocks:
            # print(f"公司 {stock_name} 代码 {stock_code}已处理，跳过 ")
            continue
        try:
            # 先计算公司文件夹的存储路径
            company_name = stock["名称"].strip()  # 去除名称两端的空格
            # 如果公司名称以 "ST" 开头，则跳过当前循环
            if company_name.startswith("ST") or company_name.startswith("*ST"):
                continue
            # 替换非法字符
            company_name_safe = company_name.replace("*", "")  # 替换 * 字符

            report_date_str = report_date.replace("-", "").replace(":", "").replace(" ", "")
            if flag:
                market = "深A股"
            else:
                market = "沪A股"
            # 写入的文件路径
            company_relative_profile_file = os.path.join(base_path, market, company_name_safe,
                                             f"{company_name_safe}_company_relative_profiles.json")
            os.makedirs(os.path.dirname(company_relative_profile_file), exist_ok=True)
            company_file = os.path.join(base_path, market, company_name_safe)

            # 尝试读取已有公司基本信息，这里获取所属行业
            latest_file_path_1 = find_latest_file(company_file, f"{company_name_safe}_profile_")
            if latest_file_path_1:
                # print(latest_file_path_1)
                profile_df = pd.DataFrame(load_json(latest_file_path_1), index=[0])
                company_fullname = profile_df.at[0, '公司名称']

                industry = profile_df.at[0, '所属行业']
            else:
                # 错误处理
                industry = '未成功获取'
                company_fullname = stock_name

            net_profit_last_three_years, revenue_last_three_years,asset_liability_ratio_last_three_years = (
                get_average_elements(company_file, stock_code))

            company_relative_profile = {
                "公司名称": company_fullname,
                "A股简称": stock_name,
                "股票代码": stock_code,
                "近三年净利润": net_profit_last_three_years,
                "近三年营业收入": revenue_last_three_years,
                "近三年资产负债率": asset_liability_ratio_last_three_years,
                # "所属概念": concept,
                "所属行业": industry,
                # "近一个月公告": announcements_last_month,
                # "员工人数": number_of_employees,
                # "管理层人数": number_of_management
            }

            company_relative_profiles.append(company_relative_profile)

            # 将每日报表保存为JSON文件
            save_to_json(company_relative_profile, company_relative_profile_file)

            # 记录已处理的股票
            processed_stocks.add(stock_code)

            # 定期保存中间结果和中断点
            if (i + 1) % 10 == 0 or i == total_stocks - 1:
                save_to_json(company_relative_profiles, company_relative_profiles_file)
                save_to_json({"processed_stocks": list(processed_stocks)}, interrupt_file)
                save_to_json(error_reports, error_reports_file)
                print(f"Progress: {i + 1}/{total_stocks} stocks processed.")

        except Exception as e:
            print(f"Error processing stock {stock_code}: {e}")
            error_reports.append({"stock_name": stock_name, "stock_code": stock_code, "flag": flag})
            continue

        # 保存最终结果
        save_to_json(company_relative_profiles, company_relative_profiles_file)
        save_to_json({"processed_stocks": list(processed_stocks)}, interrupt_file)
        save_to_json(error_reports, error_reports_file)


def get_company_relative_profiles(base_path='./stock_data/company_data', report_date=get_yesterday()):
    """
    在收盘后获取公司的每日报告，可以在开闭市的总市值与市盈率错误的情况下，读取非当日报告；但此时读取历史数据失败的错误不可接受
    :param base_path: 基本路径，默认为'./stock_data/company_data'。同样涉及到已有文件结构。
    :param report_date 指定每日报告的日期，YYYYMMDD的str，默认是昨天；若非当日闭市隔次开市前读取，市盈率一定是错的（由实时数据读取得到）
    :return: 直接将每日报告写入公司文件夹。
    """
    # 读取沪A股和深A股的数据
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
    interrupt_file = os.path.join(base_path, f'company_relative_profiles_interrupt_{report_date}.json')
    interrupt_data = load_json(interrupt_file)
    # Ensure interrupt_data is a dictionary
    if not isinstance(interrupt_data, dict):
        interrupt_data = {}
    processed_stocks = set(interrupt_data.get('processed_stocks', []))

    # 生成沪A股和深A股的每日报表
    get_company_relative_profile(sh_a_stocks, base_path, processed_stocks, 0, report_date, stock_sh_a_spot_em_df,
                                 stock_sz_a_spot_em_df, interrupt_file)
    get_company_relative_profile(sz_a_stocks, base_path, processed_stocks, 1, report_date, stock_sh_a_spot_em_df,
                                 stock_sz_a_spot_em_df, interrupt_file)


