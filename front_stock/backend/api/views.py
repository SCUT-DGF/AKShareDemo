from django.shortcuts import render

# Create your views here.

# api/views.py
from django.http import JsonResponse
import sys
import os
import json
from datetime import datetime

# 获取 src 目录的绝对路径
src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../src'))
print(src_path)

# 将 src 目录添加到 Python 的路径中
if src_path not in sys.path:
    sys.path.append(src_path)
print(sys.path)


def get_config_django():
    # django的views文件夹特供读取配置文件的函数
    # 移动文件夹时，项目路径可能需要修改
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    config_file_path = os.path.join(project_root, 'conf', 'config.json')
    with open(config_file_path, 'r') as f:
        config = json.load(f)
    return config


# 日期验证函数
def validate_date_format(date_str):
    try:
        date = datetime.strptime(date_str, "%Y%m%d")
        return date
    except ValueError:
        return None


# 导入函数
from get_weekly_report_and_daily_up2 import get_weekly_reports, check_daily_up_interface
from get_daily_reports import get_daily_reports
from get_up_limit_reports import get_merge_zt_a_data
from get_up_ah_report_data import get_up_ah_report_data
from get_company_profile import get_company_basic_profiles
from get_company_relative_profile import get_company_relative_profiles


# 定义视图
def get_weekly_reports_view(request):
    date_str = request.GET.get('date')
    if not date_str or not validate_date_format(date_str):
        return JsonResponse({'status': 'error', 'message': 'Invalid date format. Please use YYYYMMDD.'}, status=400)
    # 还要判断是否是周五的日期

    config = get_config_django()
    base_path = config['base_path']

    try:
        print(date_str, base_path)
        get_weekly_reports(base_path=base_path, report_date=date_str, date=date_str)
        return JsonResponse({'status': 'success', 'message': 'Daily reports start generating and saving.'})
    except Exception as e:
        return JsonResponse({'status': 'error', 'message': str(e)}, status=500)

def get_daily_reports_view(request):
    date_str = request.GET.get('date')
    if not date_str or not validate_date_format(date_str):
        return JsonResponse({'status': 'error', 'message': 'Invalid date format. Please use YYYYMMDD.'}, status=400)
    config = get_config_django()
    base_path = config['base_path']
    
    try:
        print(date_str, base_path)
        get_daily_reports(base_path=base_path, report_date=date_str)
        return JsonResponse({'status': 'success', 'message': 'Daily reports start generating and saving.'})
    except Exception as e:
        return JsonResponse({'status': 'error', 'message': str(e)}, status=500)


# 其他函数的视图定义类似
def check_daily_up_interface_view(request):
    date_str = request.GET.get('date')
    if not date_str or not validate_date_format(date_str):
        return JsonResponse({'status': 'error', 'message': 'Invalid date format. Please use YYYYMMDD.'}, status=400)
    config = get_config_django()
    base_path = config['base_path']

    try:
        print(date_str, base_path)
        check_daily_up_interface(base_path=base_path, date=date_str)
        return JsonResponse({'status': 'success', 'message': 'Daily up stocks information start generating and saving.'})
    except Exception as e:
        return JsonResponse({'status': 'error', 'message': str(e)}, status=500)


def get_merge_zt_a_data_view(request):
    date_str = request.GET.get('date')
    if not date_str or not validate_date_format(date_str):
        return JsonResponse({'status': 'error', 'message': 'Invalid date format. Please use YYYYMMDD.'}, status=400)
    config = get_config_django()
    base_path = config['base_path']

    try:
        print(date_str, base_path)
        get_merge_zt_a_data(base_path=base_path, report_date=date_str)
        return JsonResponse({'status': 'success', 'message': 'Daily up-limit stocks information start generating and saving.'})
    except Exception as e:
        return JsonResponse({'status': 'error', 'message': str(e)}, status=500)


def get_up_ah_report_data_view(request):
    date_str = request.GET.get('date')
    if not date_str or not validate_date_format(date_str):
        return JsonResponse({'status': 'error', 'message': 'Invalid date format. Please use YYYYMMDD.'}, status=400)
    config = get_config_django()
    base_path = config['base_path']

    try:
        print(date_str, base_path)
        get_up_ah_report_data(base_path=base_path, report_date=date_str)
        return JsonResponse({'status': 'success', 'message': 'Daily up A-H stocks information start generating and saving.'})
    except Exception as e:
        return JsonResponse({'status': 'error', 'message': str(e)}, status=500)


def get_company_basic_profile_view(request):
    date_str = request.GET.get('date')
    if not date_str or not validate_date_format(date_str):
        return JsonResponse({'status': 'error', 'message': 'Invalid date format. Please use YYYYMMDD.'}, status=400)
    config = get_config_django()
    base_path = config['base_path']

    try:
        print(date_str, base_path)
        get_company_basic_profiles(base_path=base_path, report_date=date_str)
        return JsonResponse({'status': 'success', 'message': 'Company basic profiles start generating and saving.'})
    except Exception as e:
        return JsonResponse({'status': 'error', 'message': str(e)}, status=500)


def get_company_relative_profiles_view(request):
    date_str = request.GET.get('date')
    if not date_str or not validate_date_format(date_str):
        return JsonResponse({'status': 'error', 'message': 'Invalid date format. Please use YYYYMMDD.'}, status=400)
    config = get_config_django()
    base_path = config['base_path']

    try:
        print(date_str, base_path)
        get_company_relative_profiles(base_path=base_path, report_date=date_str)
        return JsonResponse({'status': 'success', 'message': 'Company relative profiles start generating and saving.'})
    except Exception as e:
        return JsonResponse({'status': 'error', 'message': str(e)}, status=500)

