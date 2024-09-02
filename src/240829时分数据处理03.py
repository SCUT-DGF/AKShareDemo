import akshare as ak
import os
import json
import pandas as pd
from datetime import datetime, timedelta
from basic_func import load_json_df, find_latest_file_v2, load_json, save_to_json_v2, save_to_json, get_yesterday, create_dict


def generate_report_dates(begin_date, end_date):
    start = datetime.strptime(begin_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")
    report_dates = []

    while start <= end:
        if start.month in [3, 6, 9, 12] and (
                (start.month in [3, 12] and start.day == 31) or (start.month in [6, 9] and start.day == 30)):
            report_date_str = start.strftime("%Y%m%d")
            if report_date_str not in report_dates:
                report_dates.append(report_date_str)
            start += timedelta(weeks=12)
        else:
            start += timedelta(days=1)
    return report_dates


def process_report_date_data(base_path, api_list, begin_date, end_date):
    relative_base_path = os.path.join(base_path, "stock_relative")

    # 生成报表日期
    date_lists = generate_report_dates(begin_date, end_date)

    # 存储每个API最终数据的字典
    final_data_dict = {}

    for api_name, (api_func, columns) in api_list.items():
        all_data = pd.DataFrame()

        for item in date_lists:
            targeted_filename = f"{api_name}_{item}.json"  # 假设JSON扩展名
            file_path = os.path.join(relative_base_path, targeted_filename)
            df = load_json_df(file_path)

            if df.empty:
                print(f"Warning: {targeted_filename} is empty!\n")
                continue

            # 提取所需的列
            if all(col in df.columns for col in columns):
                cut_df = df[columns].copy()  # 使用.copy()以避免警告
            else:
                missing_cols = [col for col in columns if col not in df.columns]
                print(f"Warning: Some columns are missing in {targeted_filename}: {missing_cols}\n")
                continue

            if cut_df.empty:
                print(f"Warning: DataFrame {targeted_filename} after column selection is empty!\n")
                continue

            # 添加“报告期”列，使用.loc以避免SettingWithCopyWarning
            cut_df.loc[:, "报告期"] = pd.to_datetime(item, format="%Y%m%d").strftime("%Y-%m-%d")

            all_data = pd.concat([all_data, cut_df], ignore_index=True)

        # 记录每个API的数据
        final_data_dict[api_name] = all_data

    # 合并所有API的数据
    merged_data = pd.DataFrame()
    for api_name, data in final_data_dict.items():
        if merged_data.empty:
            merged_data = data
        else:
            merged_data = pd.merge(merged_data, data, on=["股票代码", "报告期"], how="outer")

    # 重新排序列
    # 确保“股票代码”和“报告期”列在前面
    columns_order = ["股票代码", "报告期"]
    # 获取所有其他列
    other_columns = [col for col in merged_data.columns if col not in columns_order]
    # 合并列顺序
    columns_order.extend(other_columns)
    # 重新排序DataFrame的列
    merged_data = merged_data[columns_order]

    # 记录处理状态
    processed_records = {api_name: {'status': 'processed', 'record_count': len(final_data_dict[api_name])} for api_name
                         in api_list}

    return merged_data, processed_records


def stock_traversal_module(func, basic_name, stock_dict, flag, args, base_path='./stock_data',
                           report_date=get_yesterday(), get_full_file=False, individual_file=True):
    """
    遍历字典，调用func获得DataFrame数据并存储。文件标识由basic_name决定。func必须有symbol或stock参数
    :param func: 调用的接口函数
    :param basic_name: 接口的基本名称，用于给各文件命名
    :param stock_dict: 遍历的股票字典，已生成的的深A或沪A股字典
    :param flag: 1表示深A股字典，0表示沪A股字典；内部需要使用不同接口
    :param args: 接口需要的股票代码外的参数
    :param base_path: 每日报表生成的基本路径
    :param report_date: 数据储存时文件的后缀id，默认为日期，为日期时可用find_latest_date函数
    :param get_full_file: bool类型，如果要将所有遍历获取的数据额外存储在一个文件中，设为True，默认为False
    :param individual_file: bool类型，数据文件存储公司文件夹还是深沪A股的大文件夹，默认为True即存入公司文件夹
    :return: 无返回值，直接写入文件并存储
    """
    company_base_path = os.path.join(base_path, "company_data")
    debug = True
    # report_date = "20240710" # 操作标识号，默认为昨天的日期
    # 加载中断点记录
    interrupt_file = os.path.join(company_base_path, f'{basic_name}_interrupt_{report_date}.json')
    interrupt_data = load_json(interrupt_file)
    if not isinstance(interrupt_data, dict):
        interrupt_data = {}
    processed_stocks = set(interrupt_data.get('processed_stocks', []))
    # 错误报告的读取
    error_file = os.path.join(company_base_path, f"{basic_name}_error_reports_{report_date}.json")
    error_reports = load_json(error_file)
    error_reports = []
    if not isinstance(error_reports, list):
        error_reports = []
    # 已处理数据的读取，用于获取完整内容
    if get_full_file:
        data_file = os.path.join(company_base_path, f"{basic_name}_full_data_{report_date}.json")
        processed_data = load_json(data_file)
        if not isinstance(processed_data, list):
            processed_data = []
    is_symbol = True
    # temp_args = inspect.signature(func).parameters
    # if "symbol" in temp_args:
    #     is_symbol = True
    # else:
    #     is_symbol = False

    # 遍历所有股票的字段
    total_stocks = len(stock_dict)
    for i, stock in enumerate(stock_dict):
        stock_code = stock['代码']
        stock_name = stock['名称']

        if debug and i >300:
            return

        # 跳过已处理的股票
        if stock_code in processed_stocks:
            # print(f"公司 {stock_name} 代码 {stock_code}已处理，跳过 ")
            continue
        try:
            # 先计算存储路径；将内容装在入对应公司文件夹中
            company_name = stock["名称"].strip()  # 去除名称两端的空格
            # 如果公司名称以 "ST" 开头，则跳过当前循环
            if company_name.startswith("ST") or company_name.startswith("*ST"):
                continue
            # 替换非法字符
            company_name_safe = company_name.replace("*", "")  # 替换 * 字符
            market = "深A股" if flag else "沪A股"
            # # 写入的文件路径
            if individual_file:
                targeted_filepath = os.path.join(company_base_path, market, company_name_safe)
            else:
                targeted_filepath = os.path.join(company_base_path, market)  # 个股信息存储的路径
            os.makedirs(os.path.join(targeted_filepath), exist_ok=True)
            filepath = os.path.join(targeted_filepath, f"{company_name_safe}_{basic_name}_{report_date}.json")

            # 通过args传递接口的其它参数
            if is_symbol:
                targeted_df = func(symbol=stock_code, company_safe_path=targeted_filepath, **args)
            else:
                targeted_df = func(stock=stock_code, **args)

            if not isinstance(targeted_df, pd.DataFrame):
                raise ValueError(f"{basic_name} does not return DataFrame ")
            if targeted_df.empty:
                print(f"Fail to fetch {stock_name}  {stock_code} data，interface：{basic_name}")
                error_reports.append({"stock_name": stock_name, "stock_code": stock_code, "flag": flag,
                                      "Error": f"From {basic_name}m empty dataframe"})
                continue
            # 确保日期字段转换为字符串格式
            for col in targeted_df.columns:
                if pd.api.types.is_datetime64_any_dtype(targeted_df[col]):
                    targeted_df[col] = targeted_df[col].apply(lambda x: x.isoformat() if pd.notnull(x) else None)
                elif pd.api.types.is_object_dtype(targeted_df[col]):
                    targeted_df[col] = targeted_df[col].astype(str)

            # 记录已处理的股票
            processed_stocks.add(stock_code)
            save_to_json_v2(targeted_df, filepath)

            combine_stock_data(base_path, stock_code, company_name_safe, market, report_date)

            # 定期保存中间结果和中断点
            if (i + 1) % 10 == 0 or i == total_stocks - 1:
                if get_full_file:
                    save_to_json(processed_data, data_file)
                save_to_json({"processed_stocks": list(processed_stocks)}, interrupt_file)
                save_to_json(error_reports, error_file)
                print(f"Progress: {i + 1}/{total_stocks} stocks processed.")

        except Exception as e:
            print(f"Error processing stock {stock_code}: {e}")
            error_reports.append({"stock_name": stock_name, "stock_code": stock_code, "flag": flag, "Error": str(e)})
            if (i + 1) % 10 == 0 or i == total_stocks - 1:
                if get_full_file:
                    save_to_json(processed_data, data_file)
                save_to_json({"processed_stocks": list(processed_stocks)}, interrupt_file)
                save_to_json(error_reports, error_file)
                print(f"Progress: {i + 1}/{total_stocks} stocks processed.")
            continue

        # 保存最终结果
        if get_full_file:
            save_to_json(processed_data, data_file)
        save_to_json({"processed_stocks": list(processed_stocks)}, interrupt_file)
        save_to_json(error_reports, error_file)


def process_full_df_data(company_safe_path, symbol, api_list_t2, begin_date, end_date, fetch_if_none=True):
    """
    未使用inspect暂时只支持symbol
    :param company_safe_path: 公司数据的基础路径
    :param symbol: 股票代码
    :param api_list_t2: 结构要求是：key: api_name value包含(func, time_column, columns)
    :param begin_date: 开始日期
    :param end_date: 结束日期
    :param fetch_if_none: 是否在文件不存在时抓取数据
    :return: 处理后的DataFrame
    """
    final_data = pd.DataFrame()
    now_date = datetime.now().strftime("%Y%m%d")

    # 遍历API列表并处理数据
    for api_name, (func, time_column, columns) in api_list_t2.items():
        targeted_filename = f"{api_name}_"  # 假设JSON扩展名
        targeted_df = load_json_df(find_latest_file_v2(company_safe_path, targeted_filename))
        # targeted_df = None
        # 如果找不到文件，并且fetch_if_none为True，则尝试抓取数据
        if targeted_df is None or targeted_df.empty:
            if fetch_if_none:
                targeted_df = func(symbol=symbol)
                if targeted_df is None or targeted_df.empty:
                    print(f"Error! Failed to find and to fetch {api_name}, stock: {symbol}")
                    return pd.DataFrame()
                else:
                    for col in targeted_df.columns:
                        if pd.api.types.is_datetime64_any_dtype(targeted_df[col]):
                            targeted_df[col] = targeted_df[col].apply(
                                lambda x: x.isoformat() if pd.notnull(x) else None)
                        elif pd.api.types.is_object_dtype(targeted_df[col]):
                            targeted_df[col] = targeted_df[col].astype(str)
                    save_to_json_v2(targeted_df, os.path.join(company_safe_path, f"{api_name}_{now_date}.json"))
            else:
                print(f"Error! Failed to find {api_name}, stock: {symbol}")
                return pd.DataFrame()

        # 过滤日期范围内的数据
        if time_column not in targeted_df.columns:
            print(f"Error! Time column {time_column} not found in {api_name} data for stock: {symbol}")
            return pd.DataFrame()
        if time_column not in columns:
            columns.append(time_column)

        # 将时间列转换为日期时间格式，并格式化为 'YYYY-mm-dd'
        targeted_df[time_column] = pd.to_datetime(targeted_df[time_column]).dt.strftime('%Y-%m-%d')

        # 根据日期范围进行筛选
        mask = (targeted_df[time_column] >= pd.to_datetime(begin_date).strftime('%Y-%m-%d')) & (
                targeted_df[time_column] <= pd.to_datetime(end_date).strftime('%Y-%m-%d'))
        filtered_df = targeted_df.loc[mask, columns].copy()

        if filtered_df.empty:
            print(f"Warning: No data found in the date range for {api_name}, stock: {symbol}")
            continue

        # 检查每列是否要丢弃
        cols_to_drop = []
        for col in filtered_df.columns:
            if col == time_column:
                continue
            null_ratio = filtered_df[col].isnull().mean()
            has_consecutive_nulls = filtered_df[col].isnull().rolling(window=5).sum().max() >= 5

            if null_ratio > 0.1 or has_consecutive_nulls:
                cols_to_drop.append(col)

        # 丢弃标记的列
        filtered_df.drop(columns=cols_to_drop, inplace=True)

        # 如果是第一次处理数据，将filtered_df赋值给final_data，否则进行合并
        if final_data.empty:
            final_data = filtered_df
        else:
            final_data = pd.merge(final_data, filtered_df, on=[time_column], how="outer")

    # 重新排序列
    if not final_data.empty:
        # 确保“股票代码”和time_column列在前面
        columns_order = [time_column]
        other_columns = [col for col in final_data.columns if col not in columns_order]
        columns_order.extend(other_columns)
        final_data = final_data[columns_order]

    return final_data


def combine_stock_data(base_path, stock_code, company_name_safe, market, report_date):
    """
    合并股票数据文件，以第一个文件的日期区间为准，将第后续文件的数据列合并到第一个文件中
    """

    def find_date_column(df):
        # 尝试找到日期列
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]) or pd.api.types.is_object_dtype(df[col]):
                try:
                    df[col] = pd.to_datetime(df[col])
                    return col
                except (ValueError, TypeError):
                    continue
        raise ValueError("没有找到有效的日期列")

    company_history_path = find_latest_file_v2(os.path.join(base_path, "company_history_data", market),
                                               name_prefix=f"{company_name_safe}_history_data")

    stock_a_indicator_filepath = os.path.join(base_path, "company_data", f"{market}",
                                              f"{company_name_safe}_stock_a_indicator_{report_date}.json")
    stock_a_indicator_filepath = find_latest_file_v2(os.path.join(base_path, "company_data", f"{market}"),
                                                     f"{company_name_safe}_stock_a_indicator_")

    merged_final_data_filepath = os.path.join(base_path, "output", "merged_final_data.json")

    output_dir = os.path.join(base_path, "output", "result")
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, f"{company_name_safe}_data_{report_date}.json")

    filepath1 = company_history_path
    other_path = {
        "stock_a_indicator": (f"{stock_a_indicator_filepath}", 1),
        "merged_final_data": (f"{merged_final_data_filepath}", 0),
    }

    # 加载数据
    combined_df = pd.DataFrame()

    for key, (filepath, type) in other_path.items():
        df2 = load_json_df(filepath)

        # type 为 1： 日度数据
        # type 为 0： 非日度数据，需要填充
        if combined_df.empty:
            combined_df = load_json_df(filepath1)
            # 找到并设置日期列
            date_col1 = find_date_column(combined_df)
            # 设置日期为索引
            combined_df.set_index(date_col1, inplace=True)

        if type:
            date_col2 = find_date_column(df2)
            df2.set_index(date_col2, inplace=True)
            # 对df2进行前向填充和后向填充，以匹配df1的日期索引
            df2_reindexed = df2.reindex(combined_df.index).ffill().bfill()
            # 合并df1和df2的数据列（除去重复的股票代码列）
            combined_df = pd.concat([combined_df, df2_reindexed], axis=1)
            # 去除重复的列
            combined_df = combined_df.loc[:, ~combined_df.columns.duplicated()]
        else:
            df2_filtered = df2[(df2['股票代码'] == stock_code)]
            date_col2 = "报告期"
            df2['报告期'] = pd.to_datetime(df2['报告期'], format='%Y-%m-%d')
            df2_filtered.set_index('报告期', inplace=True)

            # 确保 combined_df 的索引是 datetime 类型
            combined_df.index = pd.to_datetime(combined_df.index, format='%Y-%m-%d')
            # 同样，确保 df2_filtered 的索引是 datetime 类型
            df2_filtered.index = pd.to_datetime(df2_filtered.index, format='%Y-%m-%d')

            # 重新索引并填充
            df2_reindexed = df2_filtered.reindex(combined_df.index, method='ffill')

            # 合并数据
            combined_df = pd.concat([combined_df, df2_reindexed], axis=1)
            combined_df = combined_df.loc[:, ~combined_df.columns.duplicated()]

    # 重置索引，并保存为JSON
    combined_df.reset_index(inplace=True)
    combined_df[date_col1] = combined_df[date_col1].astype(str)
    save_to_json_v2(combined_df, output_file)


if __name__ == "__main__":
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_file_path = os.path.join(project_root, 'conf', 'config.json')
    # 读取配置文件
    with open(config_file_path, 'r') as f:
        config = json.load(f)
    base_path = config['base_path']

    company_base_path = os.path.join(base_path, "company_data")
    create_dict(base_path, get_H_dict=False)
    sz_dict = load_json(os.path.join(company_base_path, "sz_a_stocks.json"))
    sh_dict = load_json(os.path.join(company_base_path, "sh_a_stocks.json"))

    os.makedirs(os.path.join(base_path, "output"), exist_ok=True)

    # 定义API和其参数
    api_report_dates = {
        "stock_zcfz_em": (ak.stock_zcfz_em, ["股票代码", "资产负债率"]),
        "stock_xjll_em": (ak.stock_xjll_em, ["股票代码", "净现金流-净现金流"]),
        "stock_yjbb_em": (ak.stock_yjbb_em, ["股票代码", "净资产收益率"]),
    }
    # api_list_t2: 结构要求是：key: api_name value包含(func, time_column, columns)
    api_full_df = {
        "stock_a_indicator_lg": (ak.stock_a_indicator_lg, "trade_date", ["pe", "pb", "dv_ratio", "total_mv"])
    }
    # 处理特定日期范围的数据
    begin_date = "20210101"  # 示例开始日期
    end_date = "20231231"  # 示例结束日期

    merged_data, processed_records = process_report_date_data(base_path, api_report_dates, begin_date, end_date)
    # 保存最终数据或进行其他操作
    if not merged_data.empty:
        # output_file_path = os.path.join(base_path, "output", "merged_final_data.csv")
        # merged_data.to_csv(output_file_path, index=False)
        output_file_path = os.path.join(base_path, "output", "merged_final_data.json")
        save_to_json_v2(merged_data, output_file_path)
        print(f"Saved merged final data to {output_file_path}")
    else:
        print("No final data to save")

    # 打印处理记录
    print("Processed Records:")
    for api_name, record in processed_records.items():
        print(f"API: {api_name}, Status: {record['status']}, Record Count: {record['record_count']}")

    stock_traversal_module(
        func=process_full_df_data,  # 将处理函数作为参数传入
        basic_name="stock_a_indicator",  # 基础名称或标识符
        stock_dict=sh_dict,  # 示例股票字典
        flag=0,  # 标志位，控制处理逻辑
        args=({"api_list_t2": api_full_df, "begin_date":begin_date, "end_date": end_date}),  # 传递给处理函数的参数
        base_path=base_path,  # 数据存储基础路径
        report_date=get_yesterday(),  # 报告日期，默认为昨天
        get_full_file=False,  # 是否获取完整文件
        individual_file=True  # 是否处理单独文件
    )
    stock_traversal_module(
        func=process_full_df_data,  # 将处理函数作为参数传入
        basic_name="stock_a_indicator",  # 基础名称或标识符
        stock_dict=sz_dict,  # 示例股票字典
        flag=1,  # 标志位，控制处理逻辑
        args=({"api_list_t2": api_full_df, "begin_date":begin_date, "end_date": end_date}),  # 传递给处理函数的参数
        base_path=base_path,  # 数据存储基础路径
        report_date=get_yesterday(),  # 报告日期，默认为昨天
        get_full_file=False,  # 是否获取完整文件
        individual_file=True  # 是否处理单独文件
    )




