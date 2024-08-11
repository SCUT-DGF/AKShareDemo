import akshare as ak
import json
import os
from datetime import datetime, timedelta
import argparse
import threading
import time
import requests
from get_stock_data import get_stock_data
from get_stock_data import get_stock_data_H
from get_daily_reports04 import get_daily_reports
# from get_stock_data_realtime import get_stock_data_realtime # 迭代合并到get_stock_data
from get_company_relative_profile import get_company_relative_profiles
from get_macro_data import get_macro_data


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


def periodic_task(base_dir):
    i = 0
    filepath = os.path.join(base_dir, "company_data")
    h_filepath = os.path.join(filepath, "H_stock")
    while True:
        now = datetime.now()
        current_date = datetime.now().strftime("%Y%m%d")
        current_time = datetime.now().strftime("%H%M%S")
        # 判断是否是节假日，是的话计算休眠时间
        if is_holiday(current_date) or is_weekend(now):
            # 如果是节假日或周末，直接休眠到下一个工作日
            next_workday = now + timedelta(days=1)
            while is_holiday(next_workday.strftime("%Y%m%d")) or is_weekend(next_workday):
                next_workday += timedelta(days=1)
            sleep_seconds = (next_workday - now).total_seconds()
            time.sleep(sleep_seconds)
            continue

        # 再判断现在是否是股市开放时间，开放时间上下浮动5分钟，允许在此区间读取盘前后数据
        if ("092500" <= current_time <= "113500") or (
                "125500" <= current_time <= "150500"):
            flag_hz = True
        else:
            flag_hz = False
        if ("092500" <= current_time <= "120500") or (
                "125500" <= current_time <= "160500"):
            flag_ah = True
        else:
            flag_ah = False

        # 每隔5次重新创建一次词典
        if i == 0:
            if not flag_ah and flag_hz:
                get_stock_data(True, filepath)
            elif flag_hz:
                get_stock_data(True, filepath)
            if flag_ah:
                get_stock_data_H(False, h_filepath)
        else:
            if flag_hz:
                get_stock_data(False, filepath)
            if flag_ah:
                get_stock_data_H(False, h_filepath)

        # 更新计数器
        i = (i + 1) % 6

        # 精确计算下一次休眠的时间
        if not flag_hz and not flag_ah:
            if "090000" <= current_time <= "093000":
                # 设为9:25，则会在开市前读取一次
                next_action_time = now.replace(hour=9, minute=25, second=0, microsecond=0)
            elif "123000" <= current_time <= "130000":
                next_action_time = now.replace(hour=13, minute=0, second=0, microsecond=0)
            elif current_time >= "160000":
                next_action_time = (now + timedelta(days=1)).replace(hour=9, minute=0, second=0, microsecond=0)
                # 下面这段需要is_holiday使用API，不然跨年就有可能错，不过有元旦节好像又没事了
                while is_holiday(next_action_time.strftime("%Y%m%d")) or is_weekend(next_action_time):
                    next_action_time += timedelta(days=1)
            else:
                next_action_time = now + timedelta(minutes=30)
        else:
            # 修改频率，请修改此处
            next_action_time = now + timedelta(minutes=5)

        sleep_seconds = (next_action_time - now).total_seconds()
        time.sleep(sleep_seconds)


def hourly_task(base_dir):
    while True:
        current_time = datetime.now()
        time.sleep(3600)  # 等待1小时

def daily_task(base_dir):
    # 只支持当天闭市后获取，若到了下一天请自行调用封装好的函数，输入昨天对于的YYYYMMDD的report_date
    file_path = os.path.join(base_dir, "company_data")
    last_executed_date = None
    while True:
        now = datetime.now()
        current_date = now.strftime("%Y%m%d")

        # 判断是否是节假日或周末
        if is_holiday(current_date) or is_weekend(now):
            # 如果是节假日或周末，直接休眠到下一个工作日
            next_workday = now + timedelta(days=1)
            while is_holiday(next_workday.strftime("%Y%m%d")) or is_weekend(next_workday):
                next_workday += timedelta(days=1)
            sleep_seconds = (next_workday - now).total_seconds()
            time.sleep(sleep_seconds)
            continue

        # 判断是否是15:00之后，并且确保一天只执行一次
        if now.hour >= 15 and (last_executed_date is None or last_executed_date != current_date):
            get_daily_reports(report_date=current_date, base_path=file_path)
            # get_company_relative_profiles()
            last_executed_date = current_date

        # 休眠到下一个小时的整点
        next_hour = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
        sleep_seconds = (next_hour - now).total_seconds()
        time.sleep(sleep_seconds)

# def weekly_task(base_dir):


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


    parser = argparse.ArgumentParser(description="Fetch stock data and save to specified directory periodically.")
    parser.add_argument('--base_dir', type=str, default=base_path, help='Base directory to save the stock data')

    args = parser.parse_args()

    # 创建并启动后台线程
    task_thread = threading.Thread(target=periodic_task, args=(args.base_dir,))
    task_thread.daemon = True
    task_thread.start()

    daily_thread = threading.Thread(target=daily_task, args=(args.base_dir,))
    daily_thread.daemon = True
    daily_thread.start()

    # 获取每日报表，先放这里
    # get_daily_reports(report_date='20230630')
    # get_company_relative_profiles()

    # 保持主线程运行
    while True:
        time.sleep(1)
