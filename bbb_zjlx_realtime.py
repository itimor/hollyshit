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
    dfs = []
    for pre_code in codes:
        print(pre_code)
        c = pre_code.split('.')
        code = f'{c[1].lower()}{c[0]}'
        url = f'http://hq.sinajs.cn/list={code}'
        headers = {'User-Agent': ua.random}
        r = requests.get(url, headers=headers).text
        X = re.split('";', r)[0]
        X = re.split('="', X)[1]
        d = X.split(',')
        if d[-1] == '00':
            d_data = {
                'code': pre_code,
                'open': d[1],
                'now': d[3],
                'high': d[4],
                'low': d[5],
                'change': (float(d[3]) - float(d[1])) / (float(d[1]) + 0.0001) * 100,
                'ogc': (float(d[1]) - float(d[2])) / (float(d[2]) + 0.0001) * 100,
            }
        else:
            d_data = {
                'code': pre_code,
                'open': d[2],
                'now': d[2],
                'high': d[2],
                'low': d[2],
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
        df_a = pd.DataFrame()
        cur_t = '1600'
        if dd.hour > 8 and dd.hour < 10:
            cur_t = '0930'
            columns = ['code', 'name', 'super', 'return', 'now', 'change', 'ogc']
            df_a = new_df.loc[
                (new_df["ogc"] < -3) &
                (new_df["change"] < 5)
                , columns].sort_values(by=['ogc'], ascending=True)
            if len(df_a) > 0:
                last_df = df_a.head().round({'change': 2, 'ogc': 2}).to_string(header=None)
                chat_id = "@hollystock"
                text = '%s 昨日涨幅>5今天低开前十\n' % date + last_df
                send_tg(text, chat_id)
        if dd.hour > 13 and dd.hour < 15:
            cur_t = '1430'
            columns = ['code', 'name', 'super', 'return', 'now', 'change', 'ogc']
            df_a = new_df.loc[
                (new_df["master"] > 7) &
                (new_df["ogc"] < -2) &
                (new_df["change"] < 5)
                , columns].sort_values(by=['super'], ascending=True)
            if len(df_a) > 0:
                last_df = df_a.head().round({'change': 2, 'ogc': 2}).to_string(header=None)
                chat_id = "@hollystock"
                text = '%s 明日可能会涨\n' % date + last_df
                send_tg(text, chat_id)
        if dd.hour > 15:
            cur_t = '1600'

        d_table = f'{table_type}_new_{cur_t}'
        df_a = new_df.sort_values(by=['ogc'], ascending=True)
        df_a.to_sql(d_table, con=engine, index=False, if_exists='replace')
        print(df_a.head())


if __name__ == '__main__':
    db = 'bbb'
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
        # table_type = 'b'
        # table_type = 'c'
        for table_type in ['b', 'c']:
            s_table = f'{table_type}_new'
            main(last_d, s_table)
