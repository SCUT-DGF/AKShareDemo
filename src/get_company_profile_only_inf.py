import os
import json
import pandas as pd
import akshare as ak
from datetime import date, datetime, timedelta
from basic_func import save_to_json_v2
# from tqdm import tqdm
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


def get_stock_profile_cninfo(stock_dict, base_path, processed_stocks, flag, report_date, interrupt_file):
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

    stock_profile_cninfos_file = os.path.join(base_path, f"stock_profile_cninfos_{report_date}.json")
    error_reports_file = os.path.join(base_path, f"error_reports_{report_date}.json")

    stock_profile_cninfos = load_json(stock_profile_cninfos_file)
    error_reports = load_json(error_reports_file)

    if not isinstance(stock_profile_cninfos, list):
        stock_profile_cninfos = []

    if not isinstance(error_reports, list):
        error_reports = []

    if flag:
        market = "深A股"
        region = "深市"
    else:
        market = "沪A股"
        region = "沪市"

    report_date_str = report_date.replace("-", "").replace(":", "").replace(" ", "")
    today_str = date.today().strftime("%Y%m%d")

    fail_list_file = os.path.join(base_path, f"fail_dict_{report_date}.json")
    fail_list = load_json(fail_list_file)
    if not isinstance(fail_list, dict):
        fail_list = {}
    fail_to_fetch_stock_hist = False
    total_stocks = len(stock_dict)

    # for i, stock in tqdm(enumerate(stock_dict), total=total_stocks, desc=f"Processing stock from {region}"):
    for i, stock in enumerate(stock_dict):
        stock_code = stock['代码']
        stock_name = stock['名称']

        # 跳过已处理的股票
        if stock_code in processed_stocks:
            # print(f"公司 {stock_name} 代码 {stock_code}已处理，跳过 ")
            continue
        elif stock_code in fail_list:
            if not fail_to_fetch_stock_hist:
                print("由于股票代码在被记录在fail_list中，直接跳过。不再输出fail_list相关提示\n")
                fail_to_fetch_stock_hist = True
            continue
        try:
            # 先计算存储路径；将内容装在入对应公司文件夹中
            company_name = stock["名称"].strip()  # 去除名称两端的空格
            # 如果公司名称以 "ST" 开头，则跳过当前循环
            if company_name.startswith("ST") or company_name.startswith("*ST"):
                continue
            company_name_safe = company_name.replace("*", "")  # 替换 * 字符

            # 写入的文件路径
            stock_profile_cninfo_file = os.path.join(base_path, market, company_name_safe,
                                                      f"{company_name_safe}_stock_profile_cninfo_{report_date}.json")
            os.makedirs(os.path.dirname(stock_profile_cninfo_file), exist_ok=True)
            company_file = os.path.join(base_path, market, company_name_safe)

            profile_df = ak.stock_profile_cninfo(symbol=stock_code)
            save_to_json_v2(profile_df, os.path.join(company_file, f"{company_name_safe}_profile_{today_str}.json"))
           
            # 记录已处理的股票
            processed_stocks.add(stock_code)

            # 定期保存中间结果和中断点
            if (i + 1) % 10 == 0 or i == total_stocks - 1:
                save_to_json({"processed_stocks": list(processed_stocks)}, interrupt_file)
                save_to_json(error_reports, error_reports_file)
                if (i + 1) % frequency == 0:
                    print(f"Progress: {i + 1}/{total_stocks} stocks processed.")

        except Exception as e:
            print(f"Now the count is : {i}")
            print(f"Error processing stock {stock_code}: {e}")
            error_reports.append({"stock_name": stock_name, "stock_code": stock_code, "flag": flag})
            if (i + 1) % 10 == 0 or i == total_stocks - 1:
                save_to_json({"processed_stocks": list(processed_stocks)}, interrupt_file)
                save_to_json(error_reports, error_reports_file)
                if (i + 1) % frequency == 0:
                    print(f"Progress: {i + 1}/{total_stocks} stocks processed.")
            continue

        # 保存最终结果
        save_to_json({"processed_stocks": list(processed_stocks)}, interrupt_file)
        save_to_json(error_reports, error_reports_file)


def get_stock_profile_cninfos(base_path='./stock_data', report_date=get_yesterday()):
    """
    在收盘后获取公司的每日报告，可以在开闭市的总市值与市盈率错误的情况下，读取非当日报告；但此时读取历史数据失败的错误不可接受
    :param base_path: 基本路径，默认为'./stock_data/company_data'。同样涉及到已有文件结构。
    :param report_date 指定每日报告的日期，YYYYMMDD的str，默认是昨天；若非当日闭市隔次开市前读取，市盈率一定是错的（由实时数据读取得到）
    :return: 直接将每日报告写入公司文件夹。
    """
    print("now executing function get_stock_profile_cninfos")
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
    interrupt_file = os.path.join(company_base_path, f'stock_profile_cninfos_interrupt_{report_date}.json')
    interrupt_data = load_json(interrupt_file)
    # Ensure interrupt_data is a dictionary
    if not isinstance(interrupt_data, dict):
        interrupt_data = {}
    processed_stocks = set(interrupt_data.get('processed_stocks', []))

    # 生成沪A股和深A股的每日报表
    # 手动先获取深市
    get_stock_profile_cninfo(sz_a_stocks, company_base_path, processed_stocks, 1, report_date, interrupt_file)
    get_stock_profile_cninfo(sh_a_stocks, company_base_path, processed_stocks, 0, report_date, interrupt_file)

    print("Successfully executing function get_stock_profile_cninfos")


if __name__ == "__main__":
    is_local = True
    if is_local:
        # base_path = './stock_data'
        base_path = 'E:/Project_storage/stock_data'
    else:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        parent_dir = os.path.dirname(current_dir)
        base_path = os.path.join(parent_dir, 'data', 'stock_data')
        os.makedirs(os.path.join(parent_dir, 'data', 'stock_data'), exist_ok=True)
        os.makedirs(os.path.join(parent_dir, 'data', 'stock_data/company_history_data'), exist_ok=True)
        os.makedirs(os.path.join(parent_dir, 'data', 'stock_data/company_history_data/深A股'), exist_ok=True)
        os.makedirs(os.path.join(parent_dir, 'data', 'stock_data/company_history_data/沪A股'), exist_ok=True)
        print(base_path)
    get_stock_profile_cninfos(base_path=base_path, report_date=datetime.now().strftime("%Y%m%d"))
