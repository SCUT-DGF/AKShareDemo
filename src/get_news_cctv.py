import akshare as ak
import json
import os
from datetime import datetime, timedelta

# 给出的示例
# get_roll_yield_bar_df = ak.get_roll_yield_bar(type_method="date",var="RB",start_day="20180618",end_day="20180718")
# print(get_roll_yield_bar_df)

# 尝试：输出新闻稿，并且存入一个csv文件中


def fs_news_cctv(date, base_path):
    """
    fs: fetch and store
    宏观情报信息：新闻联播文字稿
    接口：news_cctv_df
    输入参数

    名称	类型	描述
    date	str	date="YYYYMMDD"; 时间区间为20160330-至今
    输出参数

    名称	类型	描述
    date	object	新闻日期
    title	object	新闻标题
    content	object	新闻内容

    :param date:在数据区间内的单个日期，数据区间从 20160330-至今
    :return:无返回值，获取数据后直接存储到文件中
    """
    # 将日期字符串转换为 datetime 对象，并检查是否超过当前日期
    try:
        date_obj = datetime.strptime(date, "%Y%m%d")
    except ValueError:
        raise ValueError("日期格式错误，请使用 YYYYMMDD 格式")
    today = datetime.today()
    if date_obj > today:
        raise ValueError("日期超过当前日期")
    lower_bound = datetime.strptime("20160330", "%Y%m%d")
    if lower_bound > date_obj:
        raise ValueError("日期超过支持的最早日期，最早日期是20160330")
    
    # 调用接口：
    news_cctv_df = ak.news_cctv(date=date)
    # 转换为字典列表形式
    news_list = news_cctv_df.to_dict(orient='records')
    # 存储文件路径
    # file_path = f'./news_cctv/news_cctv_{date}.json'
    file_path = f'{base_path}/news_cctv_{date}.json'
    # 检查是否为空，给出提示
    if news_cctv_df.empty:
        print(f"宏观情报信息：新闻联播文字稿 日期 {date} 对应的数据为空，可能是因为当日数据还未上传")
        return
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(news_list, f, ensure_ascii=False, indent=4)
    # 测试输出
    print(f"宏观情报信息：新闻联播文字稿 日期为{date}的数据 存储到文件：{file_path}")
    return


def fs_multiple_news_cctv(start_date, end_date):
    current_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(current_dir)
    base_path = os.path.join(parent_dir, 'data', 'stock_data/news_cctv')
    os.makedirs(os.path.join(parent_dir, 'data', 'stock_data'), exist_ok=True)
    os.makedirs(os.path.join(parent_dir, 'data', 'stock_data/news_cctv'), exist_ok=True)
    print(base_path)


    # 将日期字符串转换为 datetime 对象
    start = datetime.strptime(start_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")

    # 检查日期有效性
    if start > end:
        raise ValueError("起始日期不能晚于终止日期")
    # 检查获取的数据是否为空

    current_date = start
    while current_date <= end:
        # 将日期转换为字符串格式
        date_str = current_date.strftime("%Y%m%d")
        fs_news_cctv(date_str,base_path)
        # 日期加1天，下一个循环中获取下一天的数据
        current_date += timedelta(days=1)


if __name__ == "__main__":
    # target_date = "20240424"  # 替换为你希望获取的具体日期
    # fs_news_cctv(target_date)
    start_date = "20240601"
    end_date = "20240705"
    fs_multiple_news_cctv(start_date,end_date)