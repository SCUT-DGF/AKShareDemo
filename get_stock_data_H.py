import akshare as ak
import pandas as pd
import json
import os
from datetime import datetime

def get_stock_data_H(new_dict=True):
    """
    基本路径：base_path = r".\\stock_data\\company_data" 后续再支持自定义路径
    :param new_dict: 输入True创建新词典，False引用已有词典
    :return: 直接在对应公司文件夹中写入公司的实时股票信息。
    """
    def get_matching_h_stocks():
        """通过A+H股票字典，将沪深A股名称与对应的H股代码和名称记录下来"""
        # 获取A+H股票字典
        ah_stock_dict = ak.stock_zh_ah_name()

        # 初始化结果列表和已记录的A股名称集合
        matching_stocks = []

        # 获取沪A股和深A股的实时行情数据
        stock_sh_a_spot_df = ak.stock_sh_a_spot_em()
        stock_sz_a_spot_df = ak.stock_sz_a_spot_em()

        # 合并沪A股和深A股的实时行情数据
        stock_a_spot_df = pd.concat([stock_sh_a_spot_df, stock_sz_a_spot_df], ignore_index=True)

        # 遍历A+H股票字典，找到对应的沪深A股并记录对应的H股信息
        for _, row in ah_stock_dict.iterrows():
            a_stock_name = row['名称']
            h_stock_code = row['代码']

            # 查找沪深A股中的匹配项
            matching_a_stocks = stock_a_spot_df[stock_a_spot_df['名称'] == a_stock_name]

            # 如果有匹配的A股信息则记录对应的H股信息
            if not matching_a_stocks.empty:
                a_row = matching_a_stocks.iloc[0]
                matching_stocks.append({
                    "A股名称": a_row['名称'],
                    "A股代码": a_row['代码'],
                    "H股代码": h_stock_code
                })

        return matching_stocks

    def save_to_json(data, file_path):
        """将数据保存到JSON文件"""
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

    def load_from_json(file_path):
        """从JSON文件读取数据"""
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)

    def save_company_data_H(stock_data, base_path):
        """将每个公司的实时信息存储为单独的 JSON 文件，在基本路径内创建公司对应的文件夹，默认路径为'.\stock_data\company_data\H_stock'"""
        # 获取当前时间
        current_time = datetime.now().strftime("%Y%m%d_%H%M%S")

        # 遍历每个市场（沪A股和深A股）的数据集，按公司名称存储为单独的 JSON 文件

        for index, row in stock_data.iterrows():
            company_name = row["名称"].strip()  # 去除名称两端的空格

            # 如果公司名称以 "ST" 开头，则跳过当前循环
            if company_name.startswith("ST") or company_name.startswith("*ST"):
                continue

            # 替换非法字符
            company_name_safe = company_name.replace("*", "")  # 替换 * 字符

            company_info = {
                "代码": row["代码"],
                "名称": row["名称"],
                "最新价": row["最新价"],
                "涨跌幅": row["涨跌幅"],  # 注意单位: %
                "涨跌额": row["涨跌额"],
                "成交量": row["成交量"],  # 注意单位: 手
                "成交额": row["成交额"],  # 注意单位: 元
                "买入":row["买入"],
                "卖出": row["卖出"],
                "最高": row["最高"],
                "最低": row["最低"],
                "今开": row["今开"],
                "昨收": row["昨收"],
            }

            # 构建存储路径
            company_path = os.path.join(base_path, company_name_safe)
            os.makedirs(company_path, exist_ok=True)
            json_file = os.path.join(company_path, f"{company_name_safe}_data_{current_time}.json")

            # 将公司信息存储为 JSON 文件（指定编码为 UTF-8）
            with open(json_file, "w", encoding="utf-8") as f:
                json.dump(company_info, f, ensure_ascii=False, indent=4)

    # 创建存储数据的基本路径
    base_path = r".\stock_data\company_data\H_stock"
    os.makedirs(base_path, exist_ok=True)

    if new_dict:
        # 获取A+H股票匹配信息
        matching_h_stocks = get_matching_h_stocks()

        # 将匹配的H股信息保存到JSON文件
        save_to_json(matching_h_stocks, os.path.join(base_path, "matching_h_stocks.json"))

        # 获取沪A股数据
        stock_sh_a_spot_em_df = ak.stock_sh_a_spot_em()

        # 获取深A股数据
        stock_sz_a_spot_em_df = ak.stock_sz_a_spot_em()

        # 提取沪A股的编号、名称和代码
        sh_a_stocks = stock_sh_a_spot_em_df[['序号', '名称', '代码']].drop_duplicates().to_dict(orient='records')

        # 提取深A股的编号、名称和代码
        sz_a_stocks = stock_sz_a_spot_em_df[['序号', '名称', '代码']].drop_duplicates().to_dict(orient='records')

        # 存储沪A股和深A股的名称与代码到单独的 JSON 文件
        save_to_json(sh_a_stocks, os.path.join(base_path, "sh_a_stocks.json"))
        save_to_json(sz_a_stocks, os.path.join(base_path, "sz_a_stocks.json"))

    # 存储每个公司的实时信息到单独的 JSON 文件
    save_company_data_H(ak.stock_zh_ah_spot(), base_path)

    print("get_stock_data_H函数操作完成。")
