import json
import os
import pandas as pd
import akshare as ak
from datetime import date, datetime

# 自定义序列化器
class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()
        return super(DateEncoder, self).default(obj)

def get_macro_data(daily, monthly, yearly, output_folder='./macro_china'):
    """
    获取宏观信息，前三个标识符是布尔值，True则获取，可以选择输入存储路径
    :param daily: bool,True则获取每日更新信息
    :param monthly: bool,True则获取月度更新信息（或季度信息）
    :param yearly: bool,True则获取年度更新信息
    :param output_folder: 存储路径，默认为'./macro_china'
    :return: 直接将获取到的宏观信息写入文件中
    """
    def using_api(api_function, output_folder='./macro_china'):
        for api_name, api_func in api_functions.items():
            try:
                # 调用API函数获取数据
                data_df = api_func()
                # print(data_df.dtypes) # df.dtypes 查看类型
                # 确保 data_df 是 DataFrame 类型
                if not isinstance(data_df, pd.DataFrame):
                    raise ValueError(f"{api_name} 返回的不是 DataFrame 类型")
                # 确保日期字段转换为字符串格式
                for col in data_df.columns:
                    if pd.api.types.is_datetime64_any_dtype(data_df[col]):
                        data_df[col] = data_df[col].apply(lambda x: x.isoformat() if pd.notnull(x) else None)
                    elif pd.api.types.is_object_dtype(data_df[col]):
                        data_df[col] = data_df[col].astype(str)

                # 将DataFrame转换为字典列表
                data = data_df.to_dict(orient="records")

                # 检查 data 是否为空
                if not data:
                    raise ValueError(f"{api_name} 返回的 DataFrame 为空")
                current_time = datetime.now().strftime("%Y%m%d")
                # 定义文件路径
                filename = os.path.join(output_folder, f"{api_name}_{current_time}.json")

                # 将数据写入JSON文件
                with open(filename, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=4, cls=DateEncoder)

                print(f"接口 {api_name} 的历史信息已保存至 {filename}")
            except Exception as e:
                print(f"无法获取接口 {api_name} 的信息，错误：{e}")

    api_functions_monthly = {
        "macro_cnbs": ak.macro_cnbs,  # m
        "macro_china_qyspjg": ak.macro_china_qyspjg,  # m
        "macro_china_fdi": ak.macro_china_fdi,
        "macro_china_lpr": ak.macro_china_lpr,  # m 20号
        "macro_china_urban_unemployment": ak.macro_china_urban_unemployment,
        "macro_china_shrzgm": ak.macro_china_shrzgm,
        "macro_china_gdp_yearly": ak.macro_china_gdp_yearly,  # 季度 0715 1018 0117 0416这四天
        "macro_china_gyzjz": ak.macro_china_gyzjz,
        "macro_china_industrial_production_yoy": ak.macro_china_industrial_production_yoy,
        "macro_china_pmi_yearly": ak.macro_china_pmi_yearly,  # m
        "macro_china_cx_pmi_yearly": ak.macro_china_cx_pmi_yearly,
        "macro_china_cx_services_pmi_yearly": ak.macro_china_cx_services_pmi_yearly,
        "macro_china_non_man_pmi": ak.macro_china_non_man_pmi,
        "macro_china_fx_reserves_yearly": ak.macro_china_fx_reserves_yearly,
        "macro_china_m2_yearly": ak.macro_china_m2_yearly,
        "macro_china_new_house_price": ak.macro_china_new_house_price,
        "macro_china_enterprise_boom_index": ak.macro_china_enterprise_boom_index,  # 季度
        "macro_china_yw_electronic_index": ak.macro_china_yw_electronic_index,
        "macro_china_lpi_index": ak.macro_china_lpi_index,
        "macro_china_real_estate": ak.macro_china_real_estate,
        "macro_china_fx_gold": ak.macro_china_fx_gold,
        "macro_china_money_supply": ak.macro_china_money_supply,
        "macro_china_stock_market_cap": ak.macro_china_stock_market_cap,
        "macro_china_shibor_all": ak.macro_china_shibor_all,
        "macro_china_hk_market_info": ak.macro_china_hk_market_info,
        # # 下面六个接口疑似高频调用限制，时能调用成功：
        "macro_china_society_electricity": ak.macro_china_society_electricity,
        "macro_china_society_traffic_volume": ak.macro_china_society_traffic_volume,
        "macro_china_postal_telecommunicational": ak.macro_china_postal_telecommunicational,
        "macro_china_international_tourism_fx": ak.macro_china_international_tourism_fx,
        "macro_china_foreign_exchange_gold": ak.macro_china_foreign_exchange_gold,
        "macro_china_retail_price_index": ak.macro_china_retail_price_index,
        "macro_china_consumer_goods_retail": ak.macro_china_consumer_goods_retail,
    }
    api_functions_yearly = {
        "macro_china_reserve_requirement_ratio": ak.macro_china_reserve_requirement_ratio,  # 年度
    }
    # 假设 api_functions 是一个包含 API 名称和函数的字典
    api_functions = {
        "macro_china_construction_index": ak.macro_china_construction_index,  # 每日
        "macro_china_construction_price_index": ak.macro_china_construction_price_index,  # 每日
        "macro_china_bdti_index": ak.macro_china_bdti_index,  # 每日
        "macro_china_bsi_index": ak.macro_china_bsi_index,  # 每日
        "macro_shipping_bci": ak.macro_shipping_bci,
        "macro_shipping_bdi": ak.macro_shipping_bdi,
        "macro_shipping_bpi": ak.macro_shipping_bpi,
        "macro_shipping_bcti": ak.macro_shipping_bcti,  # 都是每日
    }
    # 接口坏了......
    api_functions2 = {
        "macro_china_swap_rate": ak.macro_china_swap_rate,  # 只能近一年，给起始日期且二者不能超一月
    }

    if daily:
        using_api(api_functions,output_folder)
    if monthly:
        using_api(api_functions_monthly,output_folder)
    if yearly:
        using_api(api_functions_yearly,output_folder)


# get_macro_data(True,True,True)
