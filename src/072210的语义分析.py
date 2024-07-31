import os
import json
import pandas as pd
import torch
from transformers import BertTokenizer, BertForSequenceClassification
from transformers import pipeline

import os
import json
import numpy as np
import pandas as pd
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

# 分割文本为指定长度的段落
def split_text(text, tokenizer, max_length=510):
    """
    将文本按照指定长度切割为多个段落
    :param text: 待切割的文本
    :param tokenizer: 分词器
    :param max_length: 每个段落的最大长度（单位为token数量）
    :return: 切割后的段落列表
    """
    tokens = tokenizer.tokenize(text)
    chunks = []
    for i in range(0, len(tokens), max_length):
        chunk = tokens[i:i + max_length]
        chunks.append(tokenizer.convert_tokens_to_string(chunk))
    return chunks


# 分析情感
def analyze_sentiment(text_chunks, pipeline):
    """
    分析文本段落的情感倾向
    :param text_chunks: 待分析的文本段落列表
    :param pipeline: 情感分析pipeline
    :return: 段落的情感标签和分数的列表
    """
    results = []
    for chunk in text_chunks:
        result = pipeline(chunk)[0]  # 每个段落单独进行情感分析
        results.append((result['label'], result['score']))
    return results


def calculate_average_sentiment(sentiment_results):
    total_score = 0
    count = 0
    for label, score in sentiment_results:
        count += 1
        if label.startswith('positive'):  # 正确解析积极情感标签
            total_score += score
        elif label.startswith('negative'):  # 正确解析消极情感标签
            total_score -= score

    average_score = total_score / count if count else 0
    overall_sentiment = 'Positive' if average_score > 0 else 'Negative'

    return overall_sentiment, abs(average_score)


# 主程序
if __name__ == "__main__":
    # 定义存放数据的文件夹路径
    is_local = True
    if is_local:
        data_folder = "./news_cctv"
    else:
        data_folder = "../data/stock_data/news_cctv" # 正确性有待测试

    # 初始化BERT tokenizer和模型
    model_name = "./bert_pretrain/roberta-base-finetuned-jd-binary-chinese"  # 本地加载
    tokenizer = BertTokenizer.from_pretrained(model_name)
    model = BertForSequenceClassification.from_pretrained(model_name)

    # 初始化情感分析pipeline
    sentiment_pipeline = pipeline('sentiment-analysis', model=model, tokenizer=tokenizer)

    # 遍历文件夹中的所有文件
    for filename in os.listdir(data_folder):
        if filename.endswith(".json"):
            file_path = os.path.join(data_folder, filename)
            news_data = load_json(file_path)

            for news_item in news_data:
                date = news_item.get('date', '')
                title = news_item.get('title', '')
                content = news_item.get('content', '')

                text_chunks = split_text(content, tokenizer)
                sentiment_results = analyze_sentiment(text_chunks, sentiment_pipeline)

                overall_sentiment, average_sentiment_score = calculate_average_sentiment(sentiment_results)

                print(f"Date: {date}")
                print(f"Title: {title}")
                print(f"Overall Sentiment: {overall_sentiment}")
                print(f"Average Score: {average_sentiment_score}")
                for i, (sentiment_label, sentiment_score) in enumerate(sentiment_results):
                    print(f"Chunk {i + 1}:")
                    print(f"Sentiment: {sentiment_label}")
                    print(f"Score: {sentiment_score}")
                    print("-" * 50)




