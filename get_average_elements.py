import akshare as ak
import pandas as pd


def get_average_elements(company_file, stock_code):
    """
    获取公司相关字段中的三个平均值
    :param company_file: 已经计算出的公司文件夹路径
    :param stock_code: 股票代码，纯数字如“000063”
    :return: 近三年净利润平均值: net_profit_last_three_years ;近三年营业收入增长平均值: revenue_growth_last_three_years; 近三年资产负债率平均值: asset_liability_ratio_last_three_years
    """
    # 定义一个函数来转换单位为数字
    def convert_to_number(value):
        if '万亿' in value:
            return float(value.replace('万亿', '')) * 1e12
        elif '亿' in value:
            return float(value.replace('亿', '')) * 1e8
        elif '万' in value:
            return float(value.replace('万', '')) * 1e4
        else:
            return float(value)


    # 获取股票代码对应的年度财务摘要信息
    stock_financial_abstract_ths_df = ak.stock_financial_abstract_ths(symbol=stock_code, indicator="按年度")

    # 选择需要的列，并将报告期列重命名为年份
    selected_columns = stock_financial_abstract_ths_df[['报告期', '净利润', '营业总收入', '资产负债率']].copy()
    selected_columns.rename(columns={'报告期': '年份'}, inplace=True)

    # 将年份转换为整数类型
    selected_columns.loc[:, '年份'] = selected_columns['年份'].astype(int)
    # 应用函数转换净利润和营业总收入列
    selected_columns['净利润'] = selected_columns['净利润'].apply(convert_to_number)
    selected_columns['营业总收入'] = selected_columns['营业总收入'].apply(convert_to_number)
    selected_columns['资产负债率'] = selected_columns['资产负债率'].str.replace('%', '').astype(float)

    # 过滤最近三年的数据
    recent_three_years = selected_columns[selected_columns['年份'] >= selected_columns['年份'].max() - 2]

    # 计算最近三年的净利润平均值
    average_net_profit = recent_three_years['净利润'].mean()

    # 计算最近三年的营业总收入增长值的平均值
    average_revenue_growth = recent_three_years['营业总收入'].mean()

    # 计算最近三年的资产负债率平均值
    average_asset_liability_ratio = recent_three_years['资产负债率'].mean()

    # 定义一个函数将数字还原为带单位的字符串
    def convert_to_unit(value):
        if value >= 1e12:
            return f"{value / 1e12:.2f}万亿"
        elif value >= 1e8:
            return f"{value / 1e8:.2f}亿"
        elif value >= 1e4:
            return f"{value / 1e4:.2f}万"
        else:
            return str(value)

    # 创建一个 DataFrame 存储这些平均值
    profile_df = pd.DataFrame({
        '近三年净利润': [convert_to_unit(average_net_profit)],
        '近三年营业收入增长': [convert_to_unit(average_revenue_growth)],
        '近三年资产负债率': [f"{average_asset_liability_ratio:.2f}%"]
    })

    net_profit_last_three_years = profile_df.at[0, '近三年净利润']
    revenue_growth_last_three_years = profile_df.at[0, '近三年营业收入增长']
    asset_liability_ratio_last_three_years = profile_df.at[0, '近三年资产负债率']

    return net_profit_last_three_years,revenue_growth_last_three_years,asset_liability_ratio_last_three_years

