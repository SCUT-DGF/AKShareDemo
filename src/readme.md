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