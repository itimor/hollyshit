# -*- coding: utf-8 -*-
# author: itimor
# 东方财富龙虎榜,并根据策略筛选股票，并发送到ding频道

from datetime import datetime, timedelta
from sqlalchemy import create_engine
import pandas as pd
import requests
import json


# 向钉钉群输出信息
def msg(text):
    token = "b18f740d516e17c03c536639d1d7d01e213aa6a9bcc85d02699c6f884867b233"
    headers = {'Content-Type': 'application/json;charset=utf-8'}  # 请求头
    api_url = f"https://oapi.dingtalk.com/robot/send?access_token={token}"
    json_text = {
        "msgtype": "text",  # 信息格式
        "text": {
            "content": text
        }
    }
    r = requests.post(api_url, json.dumps(json_text), headers=headers)
    print(r.json())


def main(date):
    columns = ['ts_code', 'name', 'close', 'amount']

    s_table = 'stock_yzzt'
    sql = f"select * from {s_table} where trade_date == '{date}' order by amount desc limit 5"
    df = pd.read_sql_query(sql, con=engine)
    text_df = df[columns].to_string(header=None)
    text1 = 'stock 一字涨停\n' + text_df

    s_table = 'stock_zt'
    sql = f"select * from {s_table} where trade_date == '{date}' and time='0935' order by amount desc limit 5"
    df = pd.read_sql_query(sql, con=engine)
    text_df = df[columns].to_string(header=None)
    text2 = 'stock 急速涨停\n' + text_df

    last_text = text1 + '\n\r' + text2
    msg(last_text)


if __name__ == '__main__':
    db = 'ccc'
    date_format = '%Y-%m-%d'
    d_format = '%Y%m%d'
    t_format = '%H%M'
    # 获得当天
    dd = datetime.now()
    # 创建连接引擎
    engine = create_engine(f'sqlite:///{db}/{db}.db', echo=False, encoding='utf-8')
    conn = engine.connect()
    trans = conn.begin()
    d = dd.strftime(d_format)
    # d = '20210326'
    main(d)
