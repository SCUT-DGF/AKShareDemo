import os
import json
import pandas as pd
import akshare as ak
from datetime import date, datetime, timedelta

# 240709 版本更新目的：增加判断当日报表是否已经生成的功能，并新增覆盖选项?
# 目的2：增加历史

class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()
        return super(DateEncoder, self).default(obj)


def save_to_json(data, path):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4, cls=DateEncoder)

def save_to_json_v2(df,path):
    json_str = json.dumps(json.loads(df.to_json(orient='records')), ensure_ascii=False, indent=4,cls=DateEncoder)
    with open(path, 'w', encoding='utf-8') as f:
        f.write(json_str)

def load_json(path):
    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}


def get_yesterday():
    return (datetime.now() - timedelta(1)).strftime('%Y%m%d')

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
                        if latest_date is None  or date > latest_date:
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
                            if(date_hms > latest_date_hms):
                                latest_date_hms = date_hms
                                latest_date = date
                                latest_file_path = os.path.join(root, file)
                except ValueError:
                    continue

    return latest_file_path

def find_suitable_file(company_file, prefix, report_date):
    latest_file_path = find_latest_file_v2(company_file, prefix,
                                             report_date, report_date)
    if latest_file_path:
        df = pd.DataFrame(load_json(latest_file_path))
        return df
    else:
        return pd.DataFrame()

def get_daily_report(stock_dict, base_path, processed_stocks, flag, report_date, stock_sh_a_spot_em_df, stock_sz_a_spot_em_df, interrupt_file):
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

    daily_reports_file = os.path.join(base_path, f"daily_reports_{report_date}.json")
    error_reports_file = os.path.join(base_path, f"error_reports_{report_date}.json")

    daily_reports = load_json(daily_reports_file)
    error_reports = load_json(error_reports_file)

    if not isinstance(daily_reports, list):
        daily_reports = []

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

        # 获取历史行情数据
        try:
            # 先计算公司文件夹的路径，可封装
            company_name = stock["名称"].strip()
            # 如果公司名称以 "ST" 开头，则跳过当前循环
            if company_name.startswith("ST") or company_name.startswith("*ST"):
                continue
            company_name_safe = company_name.replace("*", "")  # 替换非法字符，这里只替换 * 字符
            report_date_str = report_date.replace("-", "").replace(":", "").replace(" ", "")
            if flag:
                market = "深A股"
            else:
                market = "沪A股"
            # 写入的文件路径
            daily_report_file = os.path.join(base_path, market, company_name_safe,
                                             f"{company_name_safe}_daily_report_{report_date_str}.json")
            os.makedirs(os.path.dirname(daily_report_file), exist_ok=True)
            company_file = os.path.join(base_path, market, company_name_safe)

            # 暂时用前复权盖
            # latest_file_path_3 = find_latest_file_v2(company_file, f"{company_name_safe}_stock_hist_",
            #                                          report_date,report_date)
            # if latest_file_path_3:
            #     print(latest_file_path_3)
            #     stock_hist_df = pd.DataFrame(load_json(latest_file_path_3), index=[0])
            #
            # else:
            stock_hist_df = ak.stock_zh_a_hist(symbol=stock_code, period="daily", start_date=report_date,
                                               end_date=report_date, adjust="qfq")
            stock_hist_path = os.path.join(base_path, market, company_name_safe,
                                         f"{company_name_safe}_stock_hist_{report_date_str}.json")
            save_to_json_v2(stock_hist_df,stock_hist_path)

            if stock_hist_df.empty:
                print(f"无法获取公司 {stock_name} 代码 {stock_code} 的历史行情数据，对应接口：ak.stock_zh_a_hist")
                error_reports.append({"stock_name": stock_name, "stock_code": stock_code, "flag": flag})
                continue
            # print("success1")
            # 获取个股信息
            basic_prefix = "individual_info"
            stock_individual_info_em_df = find_suitable_file(company_file, f"{company_name_safe}_{basic_prefix}_", report_date)
            # print("success2")
            if stock_individual_info_em_df.empty:
                # 调用接口
                stock_individual_info_em_df = ak.stock_individual_info_em(symbol=stock_code, timeout=10)
                path = os.path.join(base_path, market, company_name_safe,
                                               f"{company_name_safe}_{basic_prefix}_{report_date_str}.json")
                save_to_json_v2(stock_individual_info_em_df, path)
            if stock_individual_info_em_df.empty:
                print(f"无法获取公司 {stock_name} 代码 {stock_code} 的个股信息，对应接口：ak.stock_individual_info_em")
                error_reports.append({"stock_name": stock_name, "stock_code": stock_code, "flag": flag})
                continue
            # print("success2")
            # 提取所需字段
            total_stock = \
                stock_individual_info_em_df.loc[stock_individual_info_em_df['item'] == '总股本', 'value'].values[0]
            total_stock = float(total_stock)
            # print("success2")
            # 提取今日市盈率和总市值（开盘和收盘）
            # flag 1深 0沪
            if not flag:
                # 这是沪A股的数据
                pe_ratio = \
                    stock_sh_a_spot_em_df.loc[stock_sh_a_spot_em_df['代码'] == stock_code, '市盈率-动态'].values[0]
                market_cap_close = (
                    stock_sh_a_spot_em_df.loc[stock_sh_a_spot_em_df['代码'] == stock_code, '总市值'].values)[0]
            else:
                # 深A股的数据
                pe_ratio = \
                    stock_sz_a_spot_em_df.loc[stock_sz_a_spot_em_df['代码'] == stock_code, '市盈率-动态'].values[0]
                market_cap_close = (
                    stock_sz_a_spot_em_df.loc[stock_sz_a_spot_em_df['代码'] == stock_code, '总市值'].values)[0]
            # print("success3")
            if not stock_hist_df.empty:
                open_price = stock_hist_df.at[0, '开盘']
                close_price = stock_hist_df.at[0, '收盘']
                change_amount = stock_hist_df.at[0, '涨跌额']
                change_percentage = stock_hist_df.at[0, '涨跌幅']
            else:
                # 错误处理
                if not flag:
                    # 这是沪A股的数据
                    open_price = \
                        stock_sh_a_spot_em_df.loc[stock_sh_a_spot_em_df['代码'] == stock_code, '今开'].values[0]
                    close_price = \
                        stock_sh_a_spot_em_df.loc[stock_sh_a_spot_em_df['代码'] == stock_code, '最新价'].values[0]
                    change_amount = \
                        stock_sh_a_spot_em_df.loc[stock_sh_a_spot_em_df['代码'] == stock_code, '5分钟涨跌'].values[0]
                    change_percentage = \
                        stock_sh_a_spot_em_df.loc[stock_sh_a_spot_em_df['代码'] == stock_code, '60日涨跌幅'].values[0]
                else:
                    # 深A股的数据
                    open_price = \
                        stock_sz_a_spot_em_df.loc[stock_sz_a_spot_em_df['代码'] == stock_code, '今开'].values[0]
                    close_price = \
                        stock_sz_a_spot_em_df.loc[stock_sz_a_spot_em_df['代码'] == stock_code, '最新价'].values[0]
                    change_amount = \
                        stock_sz_a_spot_em_df.loc[stock_sz_a_spot_em_df['代码'] == stock_code, '5分钟涨跌'].values[0]
                    change_percentage = \
                        stock_sz_a_spot_em_df.loc[stock_sz_a_spot_em_df['代码'] == stock_code, '60日涨跌幅'].values[0]
            # print("success4")

            # 尝试读取已有公司基本信息
            latest_file_path_1 = find_latest_file_v2(company_file, f"{company_name_safe}_profile_")

            if latest_file_path_1:
                # print(latest_file_path_1)
                profile_df = pd.DataFrame(load_json(latest_file_path_1), index=[0])
                # company_fullname = profile_df.at['公司名称'][0]
                company_fullname = profile_df.at[0, '公司名称']  # 使用索引0获取数据，而不是字符串索引
                # company_fullname = stock_name
            else:
                # 错误处理
                # 由于接口的高频调用限制，获取公司基本信息的接口给总计五千多次调到三分之一左右ip就被暂ban一天多了，该数据通过单独的程序获取
                company_fullname = stock_name

            # 总市值
            latest_file_path_2 = find_latest_file_v2(company_file, f"{company_name_safe}_data_", report_date)
            if latest_file_path_2:
                # print(latest_file_path_2)
                data_df = pd.DataFrame(load_json(latest_file_path_2), index=[0])
                if '总市值' in data_df.columns:
                    market_cap_open = data_df.at[0, '总市值']  # 使用索引0获取数据，而不是字符串索引
                else:
                    # 错误处理：找不到总市值列
                    # print(f"文件 {latest_file_path_2} 中缺少 '总市值' 列，无法获取正确的开盘总市值")
                    market_cap_open = market_cap_close  # 暂时直接附闭盘总市值
            else:
                # 暂时的错误处理：给它附闭盘值
                market_cap_open = market_cap_close

            daily_report = {
                "公司名称": company_fullname,
                "A股简称": stock_name,
                "股票代码": stock_code,
                "总股本": total_stock,
                "今日开盘股价": open_price,
                "今日收盘股价": close_price,
                "今日开盘总市值": market_cap_open,
                "今日收盘总市值": market_cap_close,
                "今日涨跌": change_amount,
                "今日涨跌幅": change_percentage,
                "今日收盘发行市盈率": pe_ratio
            }

            daily_reports.append(daily_report)

            keys_to_convert = ["总股本","今日开盘股价", "今日收盘股价", "今日开盘总市值", "今日收盘总市值","今日涨跌","今日涨跌幅","今日收盘发行市盈率"]
            for key in keys_to_convert:
                if key in daily_report and isinstance(daily_report[key], (float, int)):
                    if key == "总股本":
                        daily_report[key] = int(daily_report[key])
                    else:
                        daily_report[key] = int(daily_report[key] * 100)

            daily_report_file2 = os.path.join(base_path, market, company_name_safe,
                                             f"{company_name_safe}_daily_report_{report_date_str}.json")
            # 将每日报表保存为JSON文件
            save_to_json(daily_report, daily_report_file2)

            # 记录已处理的股票
            processed_stocks.add(stock_code)

            # 定期保存中间结果和中断点
            if (i + 1) % 10 == 0 or i == total_stocks - 1:
                save_to_json(daily_reports, daily_reports_file)
                save_to_json({"processed_stocks": list(processed_stocks)}, interrupt_file)
                save_to_json(error_reports, error_reports_file)
                if (i + 1) % 200 == 0 or i == total_stocks - 1:
                    print(f"This is {report_date}.")
                print(f"Progress: {i + 1}/{total_stocks} stocks processed.")


        except Exception as e:
            print(f"Error processing stock {stock_code}: {e}")
            error_reports.append({"stock_name": stock_name, "stock_code": stock_code, "flag": flag})
            continue

        # 保存最终结果
        save_to_json(daily_reports, daily_reports_file)
        save_to_json({"processed_stocks": list(processed_stocks)}, interrupt_file)
        save_to_json(error_reports, error_reports_file)


def get_daily_reports(base_path='./stock_data/company_data', report_date=get_yesterday()):
    """
    在收盘后获取公司的每日报告，可以在开闭市的总市值与市盈率错误的情况下，读取非当日报告；但此时读取历史数据失败的错误不可接受
    :param base_path: 基本路径，默认为'./stock_data/company_data'。同样涉及到已有文件结构。
    :param report_date 指定每日报告的日期，YYYYMMDD的str，默认是昨天；若非当日闭市隔次开市前读取，市盈率一定是错的（由实时数据读取得到）
    :return: 直接将每日报告写入公司文件夹。
    """

    # 先更新一遍公司的词典
    stock_sh_a_spot_em_df = ak.stock_sh_a_spot_em()
    stock_sz_a_spot_em_df = ak.stock_sz_a_spot_em()
    sh_a_stocks = stock_sh_a_spot_em_df[['序号', '名称', '代码']].drop_duplicates().to_dict(orient='records')
    sz_a_stocks = stock_sz_a_spot_em_df[['序号', '名称', '代码']].drop_duplicates().to_dict(orient='records')

    save_to_json(sh_a_stocks, os.path.join(base_path, "sh_a_stocks.json"))
    save_to_json(sz_a_stocks, os.path.join(base_path, "sz_a_stocks.json"))

    # 加载中断点记录
    interrupt_file = os.path.join(base_path, f'daily_reports_interrupt_{report_date}.json')
    interrupt_data = load_json(interrupt_file)
    # Ensure interrupt_data is a dictionary
    if not isinstance(interrupt_data, dict):
        interrupt_data = {}
    processed_stocks = set(interrupt_data.get('processed_stocks', []))

    # 生成沪A股和深A股的每日报表
    get_daily_report(sh_a_stocks, base_path, processed_stocks, 0, report_date, stock_sh_a_spot_em_df, stock_sz_a_spot_em_df, interrupt_file)
    get_daily_report(sz_a_stocks, base_path, processed_stocks, 1, report_date, stock_sh_a_spot_em_df, stock_sz_a_spot_em_df, interrupt_file)



