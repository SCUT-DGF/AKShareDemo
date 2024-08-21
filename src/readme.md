文件索引————2024-08-18

# 用户手册
## 数据获取
$\qquad$data_mining_realtime_data —— 获取时分数据与每日数据
$\qquad$数据存储路径由base_path设定，启动后将会依据设置更改config文件
$\qquad$目前运行时间为9:25-约16:32（完成所有任务）

## 可能存在问题
interrupt(中断处理文件)与error_report（错误报告，有但是前面没怎么用）会堆积在数据文件中，导致数据文件夹很杂。
输出文件路径定义可能可以优化。

## 输出数据：

### 展示的报表
#### 公司每日报表
模块定义于**get_daily_reports04.py**

输出文件地址及命名含义
$\qquad$**./stock_data/company_data**
$\qquad\qquad$daily_reports_YYYYMMDD：包含指定日期已获取到的每日报表数据（原数据）
$\qquad\qquad$daily_reports_interrupt_YYYYMMDD：用于记录中断，每处理10个股票就会写入一次，记录已处理的股票代码
$\qquad\qquad$daily_reports_errorYYYYMMDD： 生成过程中出错的股票及错误原因。出错则直接跳过。

#### 公司一周报告（输出连续涨幅的股票列表）
接口为**get_weekly_reports**，定义于**get_weekly_report_and_daily_up2.py**
**./stock_data/company_data/weekly_report**
（注：日度周度涨幅股票的字典都在weekly_report文件夹）
$\qquad$weekly_report_up_3_days_YYYYMMDD.json 连续涨幅3天
$\qquad$weekly_report_up_4_days_YYYYMMDD.json 连续涨幅4天（不包含3天）
$\qquad$weekly_report_up_5_days_YYYYMMDD.json 连续涨幅5天（不包含3天与4天，即涨幅的天数是5天）
$\qquad\qquad$都是"代码""名称"两键的股票字典
$\qquad$另外有all_weekly_report_sh_YYYYMMDD与all_weekly_report_sz_YYYYMMDD，记录每个公司连续上涨的最高天数（当周内）
$\qquad$（注：240819修复日期格式不是YYYYMMDD的问题，之前错传了错误格式的日期）

#### 每日上涨的股票（输出上涨梯度中的股票）
与每日报表相近的运行时长（本机约30min）。
接口为**check_daily_up_interface**，同样定义于

**get_weekly_report_and_daily_up2.py**
$\qquad$**./stock_data/company_data/weekly_report**
$\qquad\qquad$daily_up_report_up_range13_sz_YYYYMMDD.json 涨幅在1~3%（上涨，若下跌是-1%之类的数）
$\qquad\qquad$daily_up_report_up_range35_sz_YYYYMMDD.json 3~5%
$\qquad\qquad$daily_up_report_up_range57_sz_YYYYMMDD.json 5~7%
$\qquad\qquad$daily_up_report_up_range710_sz_YYYYMMDD.json 7~10%
$\qquad\qquad$limit_up_dict_sz_YYYYMMDD.json 涨停（上面都是深市，标记为sz）
$\qquad\qquad$daily_up_report_up_stock_sz_YYYYMMDD.json 上涨的股票字典

$\qquad$**以上都是深市字典，将sz改为sh则为沪市字典，如limit_up_dict_sh_20240819.json（这是一个具体的文件名）**

$\qquad$同样也有**总文件
all_up_range_report_sz_YYYYMMDD.json**记录个股票涨幅，**sz改为sh为则为沪市文件**

$\qquad$以上是输出前端要展示的相应字典列表

$\qquad$**对于中间产物的字典**（排除新股，或许排除创新创业股）,**路径暂时都位于"./stock_data/company_data"**
$\qquad\qquad$sz_a_stocks.json 深市A股字典
$\qquad\qquad$sz_a_stocks_excluding_new.json 深市A股字典，排除新股
$\qquad\qquad$sz_a_stocks_excluding_new_and_limit_up.json 深市A股字典，排除新股与当日涨停的股票
$\qquad\qquad$sz换为sh即为沪市的相应字典。注意，这些字典都是更新后直接覆盖的。

$\qquad\qquad$另外有一个szsh_H_stocks.json的字典，一个时间（在此前几个步骤）生成的，暂时没找到在哪生成的。估计还是中间字典

#### 每日涨停的股票信息输出
要用到每日报表数据，所以必须在每日报表获取完（没获取会在此函数中获取）再运行。
接口为**get_merge_zt_a_data**，定义于**get_up_limit_reports.py**，一般在check_daily_up_interface后运行。（不运行要多获取涨停一个字典，关系不大）
$\qquad$**./stock_data/company_data/**
$\qquad\qquad$merge_zt_a_data_YYYYMMDD.json 涨停股票的总输出信息（包含深沪两市的所有涨停股票）
$\qquad\qquad$同样有merge_zt_a_data_error_reports_20240819.json与merge_zt_a_data_interrupt_20240819.json
注：暂时没有设置公司基本信息于相关信息的日期限制，直接搜索最新日期文件，如果有不会更新
$\qquad\qquad$
#### A-H 上涨A股的A+H信息
接口为**get_up_ah_report_data**，定义于**get_up_ah_report_data.py**
$\qquad$其中使用到"daily_up_report_up_stock_sz......"与"matching_h_stocks.json"等中间文件

首先，H股时分数据存储路径示例为：**./stock_data/H_stock/公司名/时分数据文件**
另外，此函数会调用**check_daily_up_interface**，该接口即输出每日上涨的股票的接口，每日上涨股票是跟每日报表一起生成的，都很慢。A+H是每日报表生成后开的每日任务的第三个线程。


数据存储路径（输出的目标文件）
$\qquad$**./stock_data/company_data**
$\qquad\qquad$**merge_zt_a_data_YYYYMMDD.json**
$\qquad\qquad$同样有interrupt与error_report文件
（中间文件）
$\qquad$**./stock_data/company_data/weekly_report**
$\qquad\qquad$intersected_stocks_sh_YYYYMMDD.json 取上涨字典与A-H股字典的交集，含有"代码"，"名称"，"H股代码"三列。
$\qquad\qquad$sh改sz则是深市对应字典



#### 公司基本信息
单股票用**get_company_basic_profile**，遍历预定义字典用**get_company_basic_profiles**，定义于**get_company_profile.py**
总文件：
**./stock_data/company_data/** 
$\qquad$company_basic_profiles_YYYYMMDD
$\qquad\qquad$有company_basic_profiles_company_profile_interrupt_20240819

对各公司的独立文件
**./stock_data/company_data/深A股 or 沪A股/公司名/** 
$\qquad$名称示例为：**卓易信息_profile_20240812**
$\qquad$前缀为**{company_safe_name}_profile**
$\qquad\qquad$注：comapny_safe_name去除了可能的*号

注：由于关键接口的高频调用限制，暂时没有调用该接口进行批量更新。通过distribute_existing_profile.py将之前获取的公司简介等信息的总文件分散到各公司的子文件夹。

#### 公司相关信息
注："所属概念"、"近一个月公告"、"员工人数"、"管理层人数"、"与之相关的舆情信息"并未获取

单股票调用get_company_relative_profile，遍历字典调用get_company_relative_profiles，定义于
对各公司的独立文件
单股票调用输入复杂，示例：  

    get_company_relative_profile([{"代码":f"{stock_code}", "名称":f"{stock_name}"}], company_base_path, company_relative_profile_processed_stocks,flag, report_date, stock_sh_a_spot_em_df, stock_sz_a_spot_em_df, company_relative_profile_interrupt_file)
文件存储路径：
$\qquad$./stock_data/company_data/深A股 or 沪A股/公司名/
$\qquad\qquad$文件名称示例为：卓易信息_company_relative_profile_YYYYMMDD
$\qquad\qquad$前缀为{company_safe_name}_relative_profiles
$\qquad\qquad\qquad$注：Error processing stock 600650: argument of type 'bool' is not iterable之类应该是获取数据失败的报错，估计是查询接口真没那个股票的数据，一直在报Progress: XX0/2385 stocks processed.而没有全在报错就是在正常运行。如果网络出问题会直接终止运行（无法发送网络请求），服务器发回超时信息不会使程序停止。
$\qquad\qquad$

### 预测使用的非展示数据
#### 股票时分数据
接口函数为get_stock_data与get_stock_data_H，均定义于get_stock_data.py
**./stock_data/company_data/公司名**
$\qquad$公司名_data_realtime_YYYYMMDD_HHmmss.json 时分数据（文档要求输出）
$\qquad$~~adjusted_公司名_data_realtime_YYYYMMDD_HHmmss.json 时分数据部分数据规范化后的文件~~ （同下）
$\qquad$公司名_data_YYYYMMDD_HHmmss.json 时分数据原数据
$\qquad$公司名_daily_report_YYYYMMDD.json 每日报表
$\qquad$~~adjusted.....daily_report 则是对每日报表部分数据规范化后的文件~~ （使用时再进行数据处理）

$\qquad$由于不涉及到遍历字典，并没有error_report和interrupt

**H股也有对应的数据，存储路径为"./stock_data/company_data/公司名/"，只有"中集集团_data_20240805_093225.json"这一类数据**

#### 股票历史数据
**data_mining_stock_history_data.py** 获取个股每日历史数据的特供接口，输入开始结束日期，将获取文件输入到'stock_data/company_history_data'中。

company_relative_profiles 为公司相关信息字段
macro_data 为宏观数据
news_cctv 为新闻稿
这几个接口暂未汇总至获取程序中。数据通过调用相应主程序获取。请注意输出的路径。（截止240821，并没有对所有主程序的base_path进行修改。）

另外，由于公司基本信息查询接口有调用次数限制，前期读过一遍存入各公司文件夹，若要读取，借助后续的程序从company_profiles_20240702.json读入。
现有版本并不会对已获取的公司基本信息进行更新。





## 历史版本的文件分析

240801版本（对非上述内容可以做补充，没提到的没有改动）
对于二：
1.新闻联播文字稿：  get_news_cctv.py
$\qquad$前缀为 news_cctv_
$\qquad$'{base_path}/news_cctv_{date}.json'
$\qquad$其中的get_news_cctv(date, base_path)返回新闻稿的df，用于语义相关性分析。
$\qquad$fs_news_cctv(date, base_path)，fs_multiple_news_cctv(start_date, end_date)直接获取文字稿并写入文件
2.监控数据：
$\qquad$个股新闻 ak.stock_news_em
$\qquad\qquad$前缀为："stock_news_em"
$\qquad\qquad$位于遍历调用的函数字典中
$\qquad$金十板块数据监控： 提供的接口隶属宏观信息
$\qquad$阿尔戈斯全网监控与金十新闻资讯暂缺（akshare接口不再支持）   
3.中国央行决议报告 暂缺（akshare接口不再支持）   
4.整个中国宏观指数报告： get_macro_data.py
$\qquad$注：很大一部分指数月更新最新一月的数据

对于三：
1、沪深A股和H股，获取非ST股票，可以放弃ST股票的获取： 
$\qquad$合并在 get_stock_data.py ，get_stock_data_realtime.py 为四、4的要求输出，内容缩减

2、3、基本面和特色数据
$\qquad$basic_and_special_data.py
$\qquad$from basic_and_special_data import calling_func
$\qquad$调用其中的calling_func(begin_date, end_date, report_id, base_path='./stock_data/stock_relative')


对于四：
$\qquad$1.公司基础字段（一个星期更新） get_company_basic_profile   "{company_name_safe}_company_basic_profiles.json"
$\qquad$2.关联字段（一个星期）  get_company_relative_profile.py
$\qquad\qquad${company_name_safe}_company_relative_profiles.json"
$\qquad\qquad$所属概念、近一个月公告（找找好像有）、员工人员(可用tushare)、管理层人数
$\qquad$3.每日动态数据$\qquad$get_daily_reports03debug.py
$\qquad\qquad${company_name_safe}_daily_report_{report_date_str}.json
$\qquad$4.时分动态数据$\qquad$get_stock_data.py
$\qquad\qquad$f"{company_name_safe}_data_{current_time}.json"
$\qquad\qquad$f"{company_name_safe}_data_realtime_{current_time}.json"
$\qquad\qquad$f"adjusted_{company_name_safe}_data_realtime_{current_time}.json"
$\qquad\qquad$H股 f"{company_name_safe}_data_{current_time}.json"
$\qquad$5.自动标签  暂时不考虑如何实现

五：
1.输出每日报表$\qquad$get_daily_reports03debug.py 与四.3重合

2.输出一周报告
$\qquad$from get_weekly_report_and_daily_up import get_weekly_reports
$\qquad$所有股票的上涨天数：
$\qquad$f"all_weekly_report_{report_date}.json" 
$\qquad$上涨3天、4天、5天的股票代码与名称（不重合，即上涨3天只存在上涨3天的文件里；若需批量处理可以分别读取，之后合并dataframe后再使用）
$\qquad$f"{basic_name}_up_3_days_{report_date}.json"
$\qquad$f"{basic_name}_up_4_days_{report_date}.json"
$\qquad$f"{basic_name}_up_5_days_{report_date}.json"

$\qquad$输出上涨A股对应的H股
$\qquad$from get_weekly_report_and_daily_up import get_up_stock_interface
$\qquad$get_up_stock_interface(report_date)获取上涨的股票字典，之后再看A-H词典进行H股的对应（对应未完成）。
$\qquad$上涨股票字典前缀 all_up_range_report_
$\qquad$base_path /weekly_report/all_up_range_report_{report_date}.json

六的2345判断涨幅输出股票字典
$\qquad$from get_weekly_report_and_daily_up import check_daily_up_interface
$\qquad$stock_data/company_data/
$\qquad\qquad$排除新股的字典
$\qquad\qquad$sh_a_stocks_excluding_new.json 
$\qquad\qquad$sz_a_stocks_excluding_new.json
$\qquad\qquad$(排除新股之后)排除涨停股的字典
$\qquad\qquad$sh_a_stocks_excluding_new_and_limit_up.json
$\qquad\qquad$sz_a_stocks_excluding_new_and_limit_up.json


