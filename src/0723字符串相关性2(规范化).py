import os
import json
import pandas as pd
import torch
import numpy as np
import akshare as ak
import inspect
import keyboard
import time
from datetime import date, datetime, timedelta
from basic_func import DateEncoder
from basic_func import save_to_json
from basic_func import save_to_json_v2
from basic_func import load_json
from basic_func import load_json_df
from basic_func import get_yesterday
from basic_func import processing_date
from basic_func import find_latest_file
from basic_func import find_latest_file_v2
from basic_func import stock_traversal_module
from basic_func import get_matching_h_stocks
from basic_func import create_dict
from basic_func import is_holiday
from basic_func import is_weekend

from transformers import BertTokenizer, BertForSequenceClassification
from transformers import pipeline
from transformers import pipeline
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import TfidfVectorizer


# 分割文本为指定长度的段落
def split_text(text, tokenizer, max_length=510):
    tokens = tokenizer.tokenize(text)
    chunks = []
    for i in range(0, len(tokens), max_length):
        chunk = tokens[i:i + max_length]
        chunks.append(tokenizer.convert_tokens_to_string(chunk))
    return chunks


# 分析情感
def analyze_sentiment(text_chunks, pipeline):
    results = []
    for chunk in text_chunks:
        result = pipeline(chunk)[0]
        results.append((result['label'], result['score']))
    return results


# 计算文本相似度
def calculate_similarity(text1, text2):
    vectorizer = TfidfVectorizer()
    tfidf_matrix = vectorizer.fit_transform([text1, text2])
    similarity = cosine_similarity(tfidf_matrix[0:1], tfidf_matrix[1:2])
    return similarity[0][0]

from get_news_cctv import get_news_cctv

test_filepath = "E:/Project_storage/Financial_data_analysis/stock_data/company_data/深A股/鼎汉技术/鼎汉技术_stock_news_em_20240713.json"
test_df = load_json_df(test_filepath)
test_output_path = "E:/Project_storage/Financial_data_analysis/stock_data/company_data/深A股/鼎汉技术/adjusted_鼎汉技术_stock_news_em_20240713.json"
#
def find_existing_file(base_directory, name_prefix, target_date):
    """
    输入寻找的路径与文件名前缀，以及目的日期，直接返回对应df文件。
    v0版本只供新闻稿使用
    :param base_directory:
    :param name_prefix:
    :param target_date:
    :return:
    """
    df = load_json_df(find_latest_file_v2(base_directory=base_directory, name_prefix=name_prefix, before_date=target_date,
                                     after_date=target_date))
    if df.empty:
        df = get_news_cctv(target_date, base_directory)
    return df

# 主程序
if __name__ == "__main__":
    test_filepath = "E:/Project_storage/Financial_data_analysis/stock_data/company_data/深A股/鼎汉技术/鼎汉技术_stock_news_em_20240713.json"
    test_df = load_json_df(test_filepath)
    test_df['发布时间'] = pd.to_datetime(test_df['发布时间']).dt.strftime('%Y%m%d') # 日期标准化部分



    # 初始化BERT tokenizer和模型 使用huggerface
    # model_name = "bert-base-chinese"
    # tokenizer = BertTokenizer.from_pretrained(model_name)
    # model = BertForSequenceClassification.from_pretrained(model_name)
    # 本地下载
    model_name = "./bert_pretrain/roberta-base-finetuned-jd-binary-chinese"  # 本地加载
    tokenizer = BertTokenizer.from_pretrained(model_name)
    model = BertForSequenceClassification.from_pretrained(model_name)

    # 初始化情感分析pipeline
    sentiment_pipeline = pipeline('sentiment-analysis', model=model, tokenizer=tokenizer)

    for index, row in test_df.iterrows():
        news_title = row['新闻标题']
        publish_time = row['发布时间']
        print(f"新闻标题: {news_title}")
        print(f"发布时间: {publish_time}")
        # 加载新闻数据
        target_date = publish_time
        news_data_df = find_existing_file(base_directory="./news_cctv", name_prefix="news_cctv", target_date=publish_time)

        # 计算新闻标题与新闻稿内容的相关性
        results = []
        for index2, news_item in news_data_df.iterrows():
            title = news_item["title"]
            # content = news_item["content"]

            # # 分割新闻内容
            # text_chunks = split_text(content, tokenizer)

            # # 分析情感
            # sentiment_results = analyze_sentiment(text_chunks, sentiment_pipeline)
            # sentiment_label, sentiment_score = sentiment_results[0]  # 这里只取第一个段落的情感分析结果

            # 计算新闻标题与内容的相似度
            similarity = calculate_similarity(news_title, title)

            # 保存结果
            results.append({
                "新闻标题": title,
                # "情感分类": sentiment_label,
                # "情感分数": sentiment_score,
                "相似度": similarity
            })
            print(title)
            print(similarity)
        # 找到最高相似度的新闻稿
        highest_similarity_item = max(results, key=lambda x: x["相似度"])

        # 输出结果
        print("相关性最高的新闻稿标题:", highest_similarity_item["新闻标题"])
        # print("情感分类:", highest_similarity_item["情感分类"])
        # print("情感分数:", highest_similarity_item["情感分数"])
        print("相似度:", highest_similarity_item["相似度"])
