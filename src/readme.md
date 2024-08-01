0708实时数据自动采集.py，启动后可以获取实时沪深A股、H股数据，并在当天结束后生成每日报表（前复权）。该程序会根据开市时间自动计算休眠时间，包含今年内的节假日判断及周末休市的判断。
输出数据：
（中断处理部分）
./stock_data
daily_reports_YYYYMMDD：包含指定日期已获取到的每日报表数据（原数据）
daily_reports_interrupt_YYYYMMDD：用于记录中断，每处理10个股票就会写入一次，记录已处理的股票代码
error_reports_YYYYMMDD： 生成过程中出错的股票及错误原因。出错则直接跳过。

./stock_data/company_data/公司名
公司名_data_realtime_YYYYMMDD_HHmmss.json 时分数据（文档要求输出）
adjusted_公司名_data_realtime_YYYYMMDD_HHmmss.json 时分数据部分数据规范化后的文件
公司名_data_YYYYMMDD_HHmmss.json 时分数据原数据
公司名_daily_report_YYYYMMDD.json 每日报表
adjusted.....daily_report 则是对每日报表部分数据规范化后的文件

0709重用函数历史记录.py 获取个股每日历史数据的特供接口，输入开始结束日期，将获取文件输入到'stock_data/company_history_data'中。


company_relative_profiles 为公司相关信息字段
macro_data 为宏观数据
news_cctv 为新闻稿
这几个接口暂未汇总至获取程序中，需要调用其中写的函数。若要调用注意输入正确的基本路径。默认应该会创建到./src目录下。

另外，由于公司基本信息查询接口有调用次数限制，前期读过一遍存入各公司文件夹，若要读取，借助后续的程序从company_profiles_20240702.json读入。


240801
对于二：
1.新闻联播文字稿：  get_news_cctv.py
    前缀为 news_cctv_
    '{base_path}/news_cctv_{date}.json'
    其中的get_news_cctv(date, base_path)返回新闻稿的df，用于语义相关性分析。
    fs_news_cctv(date, base_path)，fs_multiple_news_cctv(start_date, end_date)直接获取文字稿并写入文件
2.监控数据：
    个股新闻 ak.stock_news_em
        前缀为："stock_news_em"
        位于遍历调用的函数字典中
    金十板块数据监控： 提供的接口隶属宏观信息
    阿尔戈斯全网监控与金十新闻资讯暂缺（akshare接口不再支持）   
3.中国央行决议报告 暂缺（akshare接口不再支持）   
4.整个中国宏观指数报告： get_macro_data.py
    注：很大一部分指数月更新最新一月的数据

对于三：
1、沪深A股和H股，获取非ST股票，可以放弃ST股票的获取： 
    合并在 get_stock_data.py ，get_stock_data_realtime.py 为四、4的要求输出，内容缩减

2、3、基本面和特色数据
    basic_and_special_data.py
    from basic_and_special_data import calling_func
    调用其中的calling_func(begin_date, end_date, report_id, base_path='./stock_data/stock_relative')
    08 01 15：52版本
        由0715基本面获取02.py、0716查询.py、0713遍历.py、0715难获取的接口(已获取未整合).py、0801难获取接口(已获取未整合).py组成
    未对基本面和特色数据做区分。区分按照获取周期划分。
    基本面和特色数据单独词典


对于四：
    1.公司基础字段（一个星期更新） get_company_basic_profile   "{company_name_safe}_company_basic_profiles.json"
    2.关联字段（一个星期）  get_company_relative_profile.py
        {company_name_safe}_company_relative_profiles.json"
        所属概念、近一个月公告（找找好像有）、员工人员(可用tushare)、管理层人数
    3.每日动态数据    get_daily_reports03debug.py
        {company_name_safe}_daily_report_{report_date_str}.json
    4.时分动态数据    get_stock_data.py
        f"{company_name_safe}_data_{current_time}.json"
        f"{company_name_safe}_data_realtime_{current_time}.json"
        f"adjusted_{company_name_safe}_data_realtime_{current_time}.json"
        H股 f"{company_name_safe}_data_{current_time}.json"
    5.自动标签  暂时不考虑如何实现

五：
1.输出每日报表    get_daily_reports03debug.py 与四.3重合

2.输出一周报告
    from get_weekly_report_and_daily_up import get_weekly_reports
    所有股票的上涨天数：
    f"all_weekly_report_{report_date}.json" 
    上涨3天、4天、5天的股票代码与名称（不重合，即上涨3天只存在上涨3天的文件里；若需批量处理可以分别读取，之后合并dataframe后再使用）
    f"{basic_name}_up_3_days_{report_date}.json"
    f"{basic_name}_up_4_days_{report_date}.json"
    f"{basic_name}_up_5_days_{report_date}.json"

    输出上涨A股对应的H股
    from get_weekly_report_and_daily_up import get_up_stock_interface
    get_up_stock_interface(report_date)获取上涨的股票字典，之后再看A-H词典进行H股的对应（对应未完成）。
    上涨股票字典前缀 all_up_range_report_
    base_path /weekly_report/all_up_range_report_{report_date}.json

六的2345判断涨幅输出股票字典
    from get_weekly_report_and_daily_up import check_daily_up_interface
    stock_data/company_data/
        排除新股的字典
        sh_a_stocks_excluding_new.json 
        sz_a_stocks_excluding_new.json
        (排除新股之后)排除涨停股的字典
        sh_a_stocks_excluding_new_and_limit_up.json
        sz_a_stocks_excluding_new_and_limit_up.json