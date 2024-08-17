import os
import json

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

print(config["last_weekly_report_date"])
print(type(config["last_weekly_report_date"]))
print(config["last_report_date"])
print(type(config["last_report_date"]))
print(config["data_status"])
print(type(config["data_status"]))
# print(config["fetching_data_date"])
# print(type(config["fetching_data_date"]))
# print(config["daily_reports_retrieved"])
# print(type(config["daily_reports_retrieved"]))

data_status = config["data_status"]
print("Fetching Data Date:", data_status["fetching_data_date"])
print("Daily Reports Retrieved:", data_status["daily_reports_retrieved"])
