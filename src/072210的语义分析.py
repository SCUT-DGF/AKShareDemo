import os
import json
import pandas as pd
import torch
from transformers import BertTokenizer, BertForSequenceClassification
from transformers import pipeline


# 加载JSON文件
def load_json(path):
    """
    从指定路径加载JSON文件
    :param path: 文件路径
    :return: dict或dataframe格式数据，读取失败返回空值。若要转dataframe可以用pd.DataFrame(~）
    """
    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}


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

                print(f"Date: {date}")
                print(f"Title: {title}")
                for i, (sentiment_label, sentiment_score) in enumerate(sentiment_results):
                    print(f"Chunk {i + 1}:")
                    print(f"Sentiment: {sentiment_label}")
                    print(f"Score: {sentiment_score}")
                    print("-" * 50)
