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
import os

ua = UserAgent()
headers = {'User-Agent': ua.random}

# 今日尾盘入  明日大涨 后天大大涨
"""
SELECT * from b_new where ogc <-2 and super > 10 and change < 6 ORDER by super;
"""
# 今日早盘入  明日大涨 后天大大涨
"""
SELECT * from b_new where ogc <0 and super < 20 and big > 0 and small < 0 and close < 10 ORDER by ogc;
"""


def get_stocks(codes):
    dfs = []
    for pre_code in codes:
        print(pre_code)
        c = pre_code.split('.')
        code = f'{c[1].lower()}{c[0]}'
        url = f'http://hq.sinajs.cn/list={code}'
        r = requests.get(url, headers=headers).text
        X = re.split('";', r)[0]
        X = re.split('="', X)[1]
        d = X.split(',')
        if d[-1] == '00':
            d_data = {
                'name': d[0],
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
                'name': d[0],
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


def main(date):
    colunms_name = ['code', 'close', 'return']
    df = ts_data.daily(ts_code='', start_date=date, end_date=date)
    new_df = df.rename(columns={'ts_code': colunms_name[0], 'close': colunms_name[1], 'pct_chg': colunms_name[2]})
    dfs = new_df[~ new_df['code'].str.contains('^300|^688|^900')]
    df_a = dfs.loc[
        (dfs["return"] > 5) &
        (dfs["close"] < 50)
        , colunms_name]
    dfs = get_stocks(df_a['code'].to_list())
    df_b = pd.merge(df_a, dfs, how='inner', left_on=['code'], right_on=['code'])
    columns = ['code', 'name', 'return', 'now', 'change', 'ogc']
    df_c = df_b.loc[
        (df_b["ogc"] < -3) &
        (df_b["change"] < 5)
        , columns].sort_values(by=['ogc'], ascending=True)
    if len(df_c) > 0:
        last_df = df_c.head().round({'change': 2, 'ogc': 2}).to_string(header=None)
        chat_id = "@hollystock"
        text = '%s 昨日涨幅>5今天低开前十\n' % date + last_df
        send_tg(text, chat_id)

    d_table = f'x_new'
    last_df = df_b.sort_values(by=['ogc'], ascending=True)
    last_df.to_sql(d_table, con=engine, index=False, if_exists='replace')
    print(last_df.head())


if __name__ == '__main__':
    db = 'tsdata'
    d_format = '%Y%m%d'
    t_format = '%H%M'
    # 获得当天
    cur_date = '20210120'
    if not os.path.exists(cur_date):
        os.makedirs(cur_date)
    ts_data = ts.pro_api('d256364e28603e69dc6362aefb8eab76613b704035ee97b555ac79ab')
    engine = create_engine(f'sqlite:///{cur_date}/{db}.db', echo=False, encoding='utf-8')
    main(cur_date)
