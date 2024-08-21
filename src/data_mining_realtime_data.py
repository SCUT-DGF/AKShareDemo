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
from combine_hourly_daily_data import combine_hourly_daily_data
from get_up_ah_report_data import get_up_ah_report_data
from get_up_limit_reports import get_merge_zt_a_data
from get_weekly_report_and_daily_up2 import check_daily_up_interface, get_weekly_reports

from basic_func import load_config, update_config

# from get_stock_data_realtime import get_stock_data_realtime # 迭代合并到get_stock_data
# from get_company_relative_profile import get_company_relative_profiles
# from get_macro_data import get_macro_data


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


def periodic_task(base_dir, config_file_path):
    print("periodic_task begins to run")
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
            if flag_hz:
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
            if "090000" <= current_time < "092500":
                # 设为9:25，则会在开市前读取一次
                next_action_time = now.replace(hour=9, minute=25, second=0, microsecond=0)
            elif "123000" <= current_time < "130000":
                next_action_time = now.replace(hour=13, minute=0, second=0, microsecond=0)
            elif current_time > "160000":
                next_action_time = (now + timedelta(days=1)).replace(hour=9, minute=0, second=0, microsecond=0)
                # 下面这段需要is_holiday使用API，不然跨年就有可能错，不过有元旦节好像又没事了
                while is_holiday(next_action_time.strftime("%Y%m%d")) or is_weekend(next_action_time):
                    next_action_time += timedelta(days=1)
            else:
                next_action_time = now + timedelta(minutes=25)
        else:
            # 修改频率，请修改此处
            next_action_time = now + timedelta(minutes=5)

        sleep_seconds = (next_action_time - now).total_seconds()
        print(f"periodic_task: next_action_time is  {next_action_time.hour}:{next_action_time.minute}:{next_action_time.second}")
        time.sleep(sleep_seconds)


def other_daily_task(base_dir, report_date):
    """
    该分支能与每日报告并行
    """
    print("other_daily_task begins to run \n")
    combine_hourly_daily_data(begin_date=report_date, end_date=report_date, base_path=base_dir,
                              name_prefix_option={"realtime_data": True, "daily_report": False})
    check_daily_up_interface(report_date, base_dir, True)

def other_daily_task2(base_dir, report_date):
    """
    另起分支的原因是，它必须等每日报告生成完，要用到每日报告的数据......
    """
    get_up_ah_report_data(report_date, base_dir)

def hourly_task(base_dir, config_file_path):
    while True:
        current_time = datetime.now()
        time.sleep(3600)  # 等待1小时


def daily_task(base_dir, config_path):
    """
    只会在每天15:00开市运行，如果提前运行则会尝试获取昨天的相关报表（如果没获取的话）
    """
    file_path = os.path.join(base_dir, "company_data")
    print("daily_task begins to run")
    while True:
        # 读取配置文件
        config = load_config(config_path)
        # last_run_time = datetime.fromisoformat(config.get('last_run_time'))
        last_report_date = datetime.fromisoformat(config.get('last_report_date')).date()
        fetching_data_date = datetime.fromisoformat(config["data_status"]['fetching_data_date']).date()
        daily_reports_retrieved = config.get('daily_reports_retrieved')

        now = datetime.now()
        current_date = now.strftime("%Y%m%d")
        yesterday_date = (now - timedelta(days=1)).strftime("%Y%m%d")
        # had_gotten_daily_reports = config.get("daily_reports_retrieved", False)

        # 判断是否是节假日或周末
        if is_holiday(current_date) or is_weekend(now):
            # 如果是节假日或周末，直接休眠到下一个工作日
            next_workday = now + timedelta(days=1)
            while is_holiday(next_workday.strftime("%Y%m%d")) or is_weekend(next_workday):
                next_workday += timedelta(days=1)
            sleep_seconds = (next_workday - now).total_seconds()
            time.sleep(sleep_seconds)
            continue

        parser_daily = argparse.ArgumentParser(
            description="Fetch stock data and save to specified directory periodically.")
        parser_daily.add_argument('--base_dir', type=str, default=base_dir,
                                  help='Base directory to save the stock data')
        parser_daily.add_argument('--report_date', type=str, default=current_date,
                                  help='A targeted date for getting data')
        args_daily = parser_daily.parse_args()



        # 判断是否是15:00之后，并且确保当天数据只获取一次
        if now.hour >= 15 and last_report_date < now.date():
            task_thread = threading.Thread(target=other_daily_task, args=(args_daily.base_dir, args_daily.report_date))
            task_thread2 = threading.Thread(target=other_daily_task2,
                                            args=(args_daily.base_dir, args_daily.report_date))
            task_thread.daemon = True
            task_thread2.daemon = True  # 都设为守护线程，即非守护线程结束时它不会阻止结束的退出，会立刻随之结束

            task_thread.start()
            if fetching_data_date == last_report_date and not daily_reports_retrieved:
                get_daily_reports(report_date=current_date, base_path=base_dir)
                config['daily_reports_retrieved'] = True
                config["data_status"]['fetching_data_date'] = now.date().isoformat()
                update_config(config_path, config)
            task_thread2.start()
            get_merge_zt_a_data(base_dir, current_date)

            # 更新配置文件
            config['last_run_time'] = now.isoformat()
            config['last_report_date'] = now.date().isoformat()
            config['daily_reports_retrieved'] = False
            update_config(config_path, config)

        # 判断是否在隔天开市前获取前一天的数据
        elif now.hour < 9 and last_report_date < (now - timedelta(days=1)).date():
            args_daily.report_date = yesterday_date
            task_thread = threading.Thread(target=other_daily_task, args=(args_daily.base_dir, args_daily.report_date))
            task_thread2 = threading.Thread(target=other_daily_task2,
                                            args=(args_daily.base_dir, args_daily.report_date))
            task_thread.daemon = True
            task_thread2.daemon = True  # 都设为守护线程，即非守护线程结束时它不会阻止结束的退出，会立刻随之结束

            task_thread.start()
            if fetching_data_date == last_report_date and not daily_reports_retrieved:
                get_daily_reports(report_date=yesterday_date, base_path=base_dir)
                config['daily_reports_retrieved'] = True
                config["data_status"]['fetching_data_date'] = (now - timedelta(days=1)).date().isoformat()
                update_config(config_path, config)
            task_thread2.start()
            get_merge_zt_a_data(base_dir, yesterday_date)

            # 更新配置文件
            config['last_report_date'] = (now - timedelta(days=1)).date().isoformat()
            config['daily_reports_retrieved'] = False
            update_config(config_path, config)

        # 直接休眠到下个15:00
        target_time_today = now.replace(hour=15, minute=0, second=0, microsecond=0)
        if now >= target_time_today:
            # 如果当前时间已经过了目标时间，设定为明天的目标时间
            target_time_today += timedelta(days=1)
        sleep_seconds = (target_time_today - now).total_seconds()
        time.sleep(sleep_seconds)


def weekly_task(base_dir, config_path):
    """
    任务将从本周一15:00开始，到下一周周五15:00之前都能获取本周的周报。
    """
    company_base_path = os.path.join(base_dir, "company_data")
    print("weekly_task begins to run")
    while True:
        # 读取配置文件
        config = load_config(config_path)
        last_weekly_report_date = datetime.fromisoformat(config.get('last_weekly_report_date')).date()

        now = datetime.now()
        # current_date = now.strftime("%Y%m%d")
        this_monday = now - timedelta(days=now.weekday())  # 本周一

        this_friday_3pm = now + timedelta(days=(4 - now.weekday()), hours=(15 - now.hour), minutes=(-now.minute),
                                          seconds=(-now.second))  # 本周五15点
        this_friday = this_friday_3pm.date()
        last_friday_3pm = this_friday_3pm - timedelta(weeks=1)  # 上周周五15点
        last_friday = last_friday_3pm.date()

        # 判断是否在获取数据的时间段内
        if now >= this_monday:
            if now >= this_friday_3pm:
                if last_weekly_report_date < this_friday:
                    # 获取本周的周报告
                    get_weekly_reports(this_friday.strftime("%Y%m%d"), this_friday.strftime("%Y%m%d"), base_dir)
                    config['last_weekly_report_date'] = this_friday.strftime("%Y-%m-%d")  # 周报日期统一定义为对应周的周五
                    update_config(config_path, config)
            else:
                if last_weekly_report_date < last_friday:
                    get_weekly_reports(last_friday.strftime("%Y%m%d"), last_friday.strftime("%Y%m%d"), base_dir)
                    config['last_weekly_report_date'] = last_friday.strftime("%Y-%m-%d")  # 周报日期统一定义为对应周的周五
                    update_config(config_path, config)

        if now < this_friday_3pm:
            sleep_seconds = (this_friday_3pm - now).total_seconds()
        else:
            sleep_seconds = (this_friday_3pm + timedelta(weeks=1) - now).total_seconds()
        time.sleep(sleep_seconds)


if __name__ == "__main__":
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_file_path = os.path.join(project_root, 'conf', 'config.json')
    data_dir_path = os.path.join(project_root, 'data')
    # 读取配置文件
    with open(config_file_path, 'r') as f:
        config = json.load(f)

    use_config_base_path = True
    if not use_config_base_path:
        use_custom_path = True
        if use_custom_path:
            base_path = 'E:/Project_storage/stock_data'
        else:
            base_path = os.path.join(data_dir_path, 'stock_data')
            os.makedirs(os.path.join(data_dir_path, 'stock_data'), exist_ok=True)
            os.makedirs(os.path.join(data_dir_path, 'stock_data/company_data'), exist_ok=True)
            os.makedirs(os.path.join(data_dir_path, 'stock_data/company_data/深A股'), exist_ok=True)
            os.makedirs(os.path.join(data_dir_path, 'stock_data/company_data/沪A股'), exist_ok=True)
            os.makedirs(os.path.join(data_dir_path, 'stock_data/company_data/H_stock'), exist_ok=True)
        print(f"Now stock_data folder path is {base_path}")

        config['base_path'] = base_path
        # 将更新的配置写回到文件
        with open(config_file_path, 'w') as f:
            json.dump(config, f, indent=4)
    else:
        base_path = config['base_path']
        print(f"Reading from config: stock_data folder path is {base_path}")

    parser = argparse.ArgumentParser(description="Fetch stock data and save to specified directory periodically.")
    parser.add_argument('--base_dir', type=str, default=base_path, help='Base directory to save the stock data')
    parser.add_argument('--config_file', type=str, default=config_file_path, help='Path to the configuration file')
    args = parser.parse_args()

    # 创建并启动获取时分数据的后台线程 注：开市日A股15:00闭市，H股16:00闭市
    task_thread = threading.Thread(target=periodic_task, args=(args.base_dir, args.config_file,))
    task_thread.daemon = True
    task_thread.start()
    # 创建并启动获取日度数据的后台线程 于每日A股闭市后开始运行
    daily_thread = threading.Thread(target=daily_task, args=(args.base_dir, args.config_file,))
    daily_thread.daemon = True
    daily_thread.start()

    weekly_thread = threading.Thread(target=weekly_task, args=(args.base_dir, args.config_file,))
    weekly_thread.daemon = True
    weekly_thread.start()

    # 获取每日报表，先放这里
    # get_daily_reports(report_date='20230630')
    # get_company_relative_profiles()

    # 保持主线程运行
    while True:
        time.sleep(1)
