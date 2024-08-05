import os
import json
import numpy as np
import pandas as pd
import akshare as ak
import inspect
from datetime import date, datetime, timedelta
from tqdm import tqdm

from basic_func import load_json_df
from basic_func import save_to_json_v2

# 获取现在的路径
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
base_path = os.path.join(parent_dir, 'data', 'stock_data')
filepath = os.path.join(base_path, "company_profiles_20240702.json")

load_json_df(filepath)
# 通过A股简称反向放入各json文件
# 但是还是涉及到沪深的分类，需要词典还是 company_data

def unpack_company_data(create_folder=True):
    current_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(current_dir)
    base_path = os.path.join(parent_dir, 'data', 'stock_data')  # 数据文件的根目录
    filepath = os.path.join(base_path, "company_profiles_20240702.json")  # 分发的目标文件路径
    date_str = filepath.split('_')[-1]
    date_str = date_str.split('.')[0]

    os.makedirs(os.path.join(base_path), exist_ok=True)
    data_path = os.path.join(base_path, "company_data")  # 存储文件所处的分支目录
    os.makedirs(data_path, exist_ok=True)
    os.makedirs(os.path.join(data_path, "深A股"), exist_ok=True)
    os.makedirs(os.path.join(data_path, "沪A股"), exist_ok=True)

    data_df = load_json_df(filepath)
    # for index, row in data_df.iterrows():
    for index, row in tqdm(data_df.iterrows(), total=len(data_df), desc="Processing companies"):
        company_name = row['A股简称']

        if company_name is None:
            print(f"Warning: 'A股简称' is None at index {index}. Skipping this row.")
            continue  # 跳过这个条目

        in_market = row['所属市场']
        if in_market.startswith("上交所"):
            market = "深A股"
        elif in_market.startswith("深交所"):
            market = "沪A股"
        else:
            print(f"market name is {in_market}!")
            continue
        # 第一层循环：将其与深A沪A尝试对应，生成另一个词典 它->沪/深->公司名，再借助这个词典或哈希把它放进去 或重新获取
        # os.makedirs()

        basic_name = "profile"
        company_name = company_name.strip()  # 去除名称两端的空格
        individual_file = True
        # 如果公司名称以 "ST" 开头，则跳过当前循环
        if company_name.startswith("ST") or company_name.startswith("*ST"):
            continue
        # 替换非法字符
        company_name_safe = company_name.replace("*", "")  # 替换 * 字符
        # # 写入的文件路径
        if individual_file:
            targeted_filepath = os.path.join(data_path, market, company_name_safe)
        else:
            targeted_filepath = os.path.join(data_path, market)  # 个股信息存储的路径

        if not os.path.exists(targeted_filepath):
            if create_folder:
                os.makedirs(targeted_filepath, exist_ok=True)
        filepath = os.path.join(targeted_filepath, f"{company_name_safe}_{basic_name}_{date_str}.json")

        company_df = pd.DataFrame([row])
        save_to_json_v2(company_df, filepath)

unpack_company_data(create_folder=True)

