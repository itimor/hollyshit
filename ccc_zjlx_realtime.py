# -*- coding: utf-8 -*-
# author: itimor
# 东方财富资金流向，并根据策略筛选股票

from datetime import datetime, timedelta
from telegram import Bot, ParseMode
from fake_useragent import UserAgent
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import tushare as ts
import requests
import re
import json

ua = UserAgent()

# 今日尾盘入  明日大涨 后天大大涨
"""
SELECT * from b_new where ogc <-2 and master > 10 and change < 6 ORDER by master;
"""
# 今日早盘入  明日大涨 后天大大涨
"""
SELECT * from b_new where ogc <0 and master < 20 and big > 0 and small < 0 and close < 10 ORDER by ogc;
"""


def get_stocks(codes):
    limit = 18  # 要发
    code_list = []
    for pre_code in codes:
        c = pre_code.split('.')
        code = f'{c[1].lower()}{c[0]}'
        code_list.append(code)
    n = int(len(codes) / limit) + 1
    m = 0
    dfs = []
    for i in range(n):
        a = code_list[m:m + limit]
        print(a)
        m = (i + 1) * limit
        url = f"http://hq.sinajs.cn/list={','.join(a)}"
        headers = {'User-Agent': ua.random}
        r = requests.get(url, headers=headers).text
        X = re.split('";', r)
        for x in X[:-1]:
            y = re.split('="', x)
            Y = re.split('hq_str_', y[0])[1]
            pre_code = f'{Y[2:]}.{Y[:2].upper()}'
            d = y[1].split(',')
            if d[-1] == '00':
                d_data = {
                    'code': pre_code,
                    'open': d[1],
                    'now': d[3],
                    'change': (float(d[3]) - float(d[1])) / (float(d[1]) + 0.0001) * 100,
                    'ogc': (float(d[1]) - float(d[2])) / (float(d[2]) + 0.0001) * 100,
                }
            else:
                d_data = {
                    'code': pre_code,
                    'open': d[2],
                    'now': d[2],
                    'change': 0.0,
                    'ogc': 0.0,
                }
            dfs.append(d_data)
    dfs_json = json.dumps(dfs)
    df_a = pd.read_json(dfs_json, orient='records')
    return df_a


def send_tg(text, chat_id):
    token = '723532221:AAH8SSfM7SfTe4HmhV72QdLbOUW3akphUL8'
    bot = Bot(token=token)
    chat_id = chat_id
    bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.HTML)


def main(date, s_table):
    df = pd.read_sql_query(f'select * from {s_table}', con=engine)
    dfs = get_stocks(df['code'].to_list())
    if len(dfs) > 0:
        new_df = pd.merge(df, dfs, how='inner', left_on=['code'], right_on=['code'])
        if dd.hour > 15:
            df_a = new_df.sort_values(by=['ogc'], ascending=True)
            df_a.to_sql(s_table, con=engine, index=False, if_exists='replace')
            print(df_a.head())


if __name__ == '__main__':
    db = 'ccc'
    d_format = '%Y%m%d'
    t_format = '%H%M'
    # 获得当天
    dd = datetime.now()
    start_date = dd - timedelta(days=10)
    end_date = dd - timedelta(days=1)
    cur_t = dd.strftime(t_format)
    if dd.hour > 8:
        # ts初始化
        ts_data = ts.pro_api('d256364e28603e69dc6362aefb8eab76613b704035ee97b555ac79ab')
        df = ts_data.trade_cal(exchange='', start_date=start_date.strftime(d_format),
                               end_date=end_date.strftime(d_format), is_open='1')
        last_d = df.tail(1)['cal_date'].to_list()[0]
        # last_d = "20210116"
        # 创建连接引擎
        engine = create_engine(f'sqlite:///{last_d}/{db}.db', echo=False, encoding='utf-8')
        s_table = f'b_new'
        main(last_d, s_table)

