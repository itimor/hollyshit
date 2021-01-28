# -*- coding: utf-8 -*-
# author: itimor
# 东方财富资金流向，并根据策略筛选股票

from datetime import datetime, timedelta
from telegram import Bot, ParseMode
from fake_useragent import UserAgent
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd
import numpy as np
import tushare as ts
import requests
import re
import json
import random

ua = UserAgent()
# 今日尾盘入  明日大涨 后天大大涨
"""
SELECT * from b_new where ogc <-2 and master > 10 and change < 6 ORDER by master;
"""
# 今日早盘入  明日大涨 后天大大涨
"""
SELECT * from b_new where ogc <0 and master < 20 and big > 0 and small < 0 and close < 10 ORDER by ogc;
SELECT * from b_new_0930 where abs(ogc) <1 AND master < 10 AND master > 4 AND super < 12 AND super > 0 AND big < 0 AND small < 0 AND mid < 0 AND change < 5 ORDER by master;
"""


# 获取实时行情-新浪接口
def get_stocks_by_sina(codes):
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
                    'now': d[3],
                }
            else:
                d_data = {
                    'code': pre_code,
                    'now': d[2],
                }

            dfs.append(d_data)
    dfs_json = json.dumps(dfs)
    df_a = pd.read_json(dfs_json, orient='records')
    return df_a


# 获取实时行情-腾讯接口
def get_stocks_by_qq(codes):
    limit = 18  # 要发
    code_list = []
    for pre_code in codes:
        c = pre_code.split('.')
        code = f's_{c[1].lower()}{c[0]}'
        code_list.append(code)
    n = int(len(codes) / limit) + 1
    m = 0
    dfs = []
    for i in range(n):
        a = code_list[m:m + limit]
        m = (i + 1) * limit
        s = '%.13f' % random.random()
        url = f"http://qt.gtimg.cn/r={s}q={','.join(a)}"
        headers = {'User-Agent': ua.random}
        r = requests.get(url, headers=headers).text
        X = re.split('";', r)
        for x in X[:-1]:
            y = re.split('="', x)
            Y = re.split('v_s_', y[0])[1]
            pre_code = f'{Y[2:]}.{Y[:2].upper()}'
            d = y[1].split('~')
            d_data = {
                'code': pre_code,
                'now': d[3],
                'open': float(d[3]) - float(d[4]),
            }
            dfs.append(d_data)
    dfs_json = json.dumps(dfs)
    df_a = pd.read_json(dfs_json, orient='records')
    return df_a


# 获取实时行情-网易接口
def get_stocks_by_126(codes):
    limit = 18  # 要发
    code_list = []
    for pre_code in codes:
        c = pre_code.split('.')
        if c[1] == 'SH':
            pre = '0'
        else:
            pre = '1'
        code = f'{pre}{c[0]}'
        code_list.append(code)
    n = int(len(codes) / limit) + 1
    m = 0
    dfs = []
    for i in range(n):
        a = code_list[m:m + limit]
        m = (i + 1) * limit
        url = f"http://api.money.126.net/data/feed/{','.join(a)}"
        headers = {'User-Agent': ua.random}
        r = requests.get(url, headers=headers).text
        X = re.split('[)];', r)
        X = re.split('_ntes_quote_callback[(]', X[0])
        b = json.loads(X[1])
        for d in b.values():
            Y = d['code']
            if Y[:1] == 0:
                pre = 'SH'
            else:
                pre = 'SZ'
            pre_code = f'{Y[1:]}.{pre}'
            if d['status'] == 0:
                d_data = {
                    'code': pre_code,
                    'open': d['open'],
                    'now': d['price'],
                }
            else:
                d_data = {
                    'code': pre_code,
                    'open': d['yestclose'],
                    'now': d['yestclose'],
                }
            dfs.append(d_data)
    dfs_json = json.dumps(dfs)
    df_a = pd.read_json(dfs_json, orient='records')
    print(df_a)
    return df_a


def send_tg(text, chat_id):
    token = '723532221:AAH8SSfM7SfTe4HmhV72QdLbOUW3akphUL8'
    bot = Bot(token=token)
    chat_id = chat_id
    bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.HTML)


def main(date, s_table):
    sql = f"select * from {s_table} where create_date = '{date}'"
    df = pd.read_sql_query(sql, con=engine)
    df.drop(['open'], axis=1, inplace=True)
    if len(df) == 0:
        return
    dfs = get_stocks_by_qq(df['code'].to_list())
    if len(dfs) > 0:
        new_df = pd.merge(df, dfs, how='inner', left_on=['code'], right_on=['code'])
        cur_t = '0930'
        if dd.hour == 9:
            cur_t = '0930'
            change = f'change_{cur_t}'
            ogc = f'ogc_{cur_t}'
            try:
                new_df[change] = (new_df['now'] - new_df['open']) / new_df['open'] * 100
                new_df[ogc] = (new_df['open'] - new_df['close']) / new_df['close'] * 100
            except:
                new_df[change] = 0
                new_df[ogc] = 0
            columns = ['code', 'name', 'master', 'return', change, ogc]
            df_a = new_df.loc[
                (new_df[ogc] < -3) &
                (new_df[change] < 5)
                , columns].sort_values(by=[ogc], ascending=True)
            if len(df_a) > 0:
                last_df = df_a.head().round({change: 2, ogc: 2}).to_string(header=None)
                chat_id = "@hollystock"
                text = '%s 早盘预计会涨\n' % date + last_df
                send_tg(text, chat_id)
        if dd.hour == 10:
            cur_t = '1030'
            change = f'change_{cur_t}'
            ogc = f'ogc_{cur_t}'
            try:
                new_df[change] = (new_df['now'] - new_df['open']) / new_df['open'] * 100
                new_df[ogc] = (new_df['open'] - new_df['close']) / new_df['close'] * 100
            except:
                new_df[change] = 0
                new_df[ogc] = 0
            columns = ['code', 'name', 'master', 'return', change, ogc]
            df_a = new_df.loc[
                (new_df[ogc] < 1) &
                (new_df[ogc] > -0.5) &
                (new_df[ogc] != 0) &
                (new_df["master"] < 10) &
                (new_df["master"] > 4) &
                (new_df["super"] < 12) &
                (new_df["super"] > 0) &
                (new_df["mid"] < 0) &
                (new_df["small"] < 0) &
                (new_df["big"] < 0) &
                (new_df[change] < 5)
                , columns].sort_values(by=[ogc], ascending=True)
            if len(df_a) > 0:
                last_df = df_a.head().round({change: 2, ogc: 2}).to_string(header=None)
                chat_id = "@hollystock"
                text = '%s 早盘预计会涨\n' % date + last_df
                send_tg(text, chat_id)
        if dd.hour == 14:
            cur_t = '1430'
            change = f'change_{cur_t}'
            ogc = f'ogc_{cur_t}'
            try:
                new_df[change] = (new_df['now'] - new_df['open']) / new_df['open'] * 100
                new_df[ogc] = (new_df['open'] - new_df['close']) / new_df['close'] * 100
            except:
                new_df[change] = 0
                new_df[ogc] = 0
            columns = ['code', 'name', 'master', 'return', change, ogc]
            df_a = new_df.loc[
                (new_df["master"] > 7) &
                (new_df[ogc] < -2) &
                (new_df[change] < 5)
                , columns].sort_values(by=['master'], ascending=True)
            if len(df_a) > 0:
                last_df = df_a.head().round({change: 2, ogc: 2}).to_string(header=None)
                chat_id = "@hollystock"
                text = '%s 尾盘预计会涨\n' % date + last_df
                send_tg(text, chat_id)
        if dd.hour > 15:
            cur_t = '1630'
        change = f'change_{cur_t}'
        ogc = f'ogc_{cur_t}'
        try:
            new_df[change] = (new_df['now'] - new_df['open']) / new_df['open'] * 100
            new_df[ogc] = (new_df['open'] - new_df['close']) / new_df['close'] * 100
        except:
            new_df[change] = 0
            new_df[ogc] = 0
        df_a = new_df.sort_values(by=[ogc], ascending=True).set_index('create_date')
        df_a.drop(['now'], axis=1, inplace=True)
        print(df_a.head())
        try:
            # delete those rows that we are going to "upsert"
            engine.execute(f"delete from {s_table} where create_date = '{date}'")
            trans.commit()

            # insert changed rows
            df_a.to_sql(s_table, engine, if_exists='append', index=True)
        except:
            trans.rollback()
            raise


if __name__ == '__main__':
    db = 'bbb'
    date_format = '%Y-%m-%d'
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
        cur_date = datetime.strptime(last_d, d_format)
        last_date = cur_date.strftime(date_format)
        # 创建连接引擎
        engine = create_engine(f'sqlite:///{db}/{db}.db', echo=False, encoding='utf-8')
        conn = engine.connect()
        trans = conn.begin()
        s_table = f'zjlx'
        main(last_date, s_table)
