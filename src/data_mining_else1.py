import akshare as ak
import json
import os
from datetime import datetime, timedelta
import argparse
import threading
import time
import requests

from get_daily_reports04 import get_daily_reports

from get_news_cctv import fs_news_cctv
from get_news_cctv import fs_multiple_news_cctv
from get_macro_data import get_macro_data

from basic_and_special_data import calling_func

from get_company_profile import get_company_basic_profiles
from get_company_relative_profile import get_company_relative_profiles

from get_weekly_report_and_daily_up2 import get_weekly_reports
from get_weekly_report_and_daily_up2 import check_daily_up_interface

# 暂：24年的节假日列表，最好用外部API获取
holidays = [
    "20240101",  # 元旦
    "20240210", "20240211", "20240212", "20240213", "20240214", "20240215", "20240216",  # 春节
    "20240405",  # 清明节
    "20240501",  # 劳动节
    "20240609", "20240610", "20240611",  # 端午节
    "20240913", "20240914", "20240915",  # 中秋节
    "20241001", "20241002", "20241003", "20241004", "20241005", "20241006", "20241007",  # 国庆节
]

def is_holiday(date_str):
    return date_str in holidays

def is_weekend(date):
    return date.weekday() >= 5  # 5: Saturday, 6: Sunday


if __name__ == "__main__":

    is_local = True
    if is_local:
        base_path = './stock_data'
    else:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        parent_dir = os.path.dirname(current_dir)
        base_path = os.path.join(parent_dir, 'data', 'stock_data')
        os.makedirs(os.path.join(parent_dir, 'data', 'stock_data'), exist_ok=True)
        os.makedirs(os.path.join(parent_dir, 'data', 'stock_data/company_history_data'), exist_ok=True)
        os.makedirs(os.path.join(parent_dir, 'data', 'stock_data/company_history_data/深A股'), exist_ok=True)
        os.makedirs(os.path.join(parent_dir, 'data', 'stock_data/company_history_data/沪A股'), exist_ok=True)
        print(base_path)

        begin_date = "20240729"
        end_date = "20240801"
        report_id = "20240801"
        daily = True
        weekly = True
        monthly = True

        from get_news_cctv import fs_news_cctv
        from get_news_cctv import fs_multiple_news_cctv
        from get_macro_data import get_macro_data
        from basic_and_special_data import calling_func
        from get_company_profile import get_company_basic_profiles
        from get_company_relative_profile import get_company_relative_profiles
        from get_weekly_report_and_daily_up2 import get_weekly_reports
        from get_weekly_report_and_daily_up2 import get_up_stock_interface
        from get_weekly_report_and_daily_up2 import check_daily_up_interface

        getting_news_cctv = True if daily else False
        getting_macro_data = True if monthly else False
        getting_basic_data = True if weekly else False
        getting_special_data = True if weekly else False
        getting_basic_profile = True if weekly else False
        getting_relative_profile = True if weekly else False
        getting_weekly_reports = True if weekly else False
        getting_up_stock = True if daily else False
        getting_check_up = True if daily else False


        if getting_news_cctv:
            news_cctv_filepath = ""
            # 路径确定放在函数里了
            fs_multiple_news_cctv(start_date=begin_date,end_date=end_date)
        if getting_macro_data:
            macro_data_filepath = os.path.join(base_path, "macro_data")
            get_macro_data(daily=daily, monthly=monthly, yearly=monthly,  output_folder=macro_data_filepath)

        if getting_basic_data or getting_special_data:
            basic_special_data_filepath = os.path.join(base_path, "stock_relative")
            # Fetch basic and special data
            calling_func(begin_date=begin_date,end_date=end_date, report_id=report_id, base_path=basic_special_data_filepath)

        if getting_basic_profile:
            basic_profile_filepath = os.path.join(base_path, "company_data")
            # Fetch basic company profiles
            get_company_basic_profiles(base_path=basic_profile_filepath)

        if getting_relative_profile:
            relative_profile_filepath = os.path.join(base_path, "stock_relative")
            # Fetch relative company profiles
            get_company_relative_profiles(base_path=relative_profile_filepath, report_date=report_id)

        if getting_weekly_reports:
            weekly_reports_filepath = os.path.join(base_path, "company_data")
            # Fetch weekly reports
            get_weekly_reports(date=end_date, report_date=report_id, company_base_path=weekly_reports_filepath)

        if getting_check_up:
            check_up_filepath = os.path.join(base_path, "company_data")
            # Check daily up interface data
            check_daily_up_interface(date=end_date, base_path=check_up_filepath, creating_new_dict=True)

        if getting_up_stock:
            up_stock_filepath = os.path.join(base_path, "company_data")
            # Fetch up stock interface data
            get_up_stock_interface(date=end_date, base_path=up_stock_filepath)

