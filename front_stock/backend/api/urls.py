# backend/api/urls.py
from django.urls import path
from .views import (
    get_weekly_reports_view,
    get_daily_reports_view,
    check_daily_up_interface_view,
    get_merge_zt_a_data_view,
    get_up_ah_report_data_view,
    get_company_basic_profile_view,
    get_company_relative_profiles_view
)

urlpatterns = [
    path('weekly-reports/', get_weekly_reports_view, name='get_weekly_reports'),
    path('daily-reports/', get_daily_reports_view, name='get_daily_reports'),
    path('check-daily-up/', check_daily_up_interface_view, name='check_daily_up_interface'),
    path('merge-zt-a/', get_merge_zt_a_data_view, name='get_merge_zt_a_data'),
    path('up-ah-report/', get_up_ah_report_data_view, name='get_up_ah_report_data'),
    path('company-basic/', get_company_basic_profile_view, name='get_company_basic_profile'),
    path('company-relative/', get_company_relative_profiles_view, name='get_company_relative_profiles'),
]
