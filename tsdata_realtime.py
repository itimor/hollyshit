# -*- coding: utf-8 -*-
# author: itimor
# 东方财富资金流向，并根据策略筛选股票

from datetime import datetime, timedelta
from telegram import Bot, ParseMode
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd
import numpy as np
import tushare as ts
import requests
import re
import json
import random

# 今日尾盘入  明日大涨 后天大大涨
"""
SELECT * from b_new where ogc <-2 and master > 10 and change < 6 ORDER by master;
"""
# 今日早盘入  明日大涨 后天大大涨
"""
SELECT * from b_new where ogc <0 and master < 20 and big > 0 and small < 0 and close < 10 ORDER by ogc;
SELECT * from b_new_0930 where abs(ogc) <1 AND master < 10 AND master > 4 AND super < 12 AND super > 0 AND big < 0 AND small < 0 AND mid < 0 AND change < 5 ORDER by master;
"""


def get_stocks():
    colunms_name = ['code', 'open', 'now']
    df = ts_data.daily(ts_code='', start_date=next_d, end_date=next_d)
    new_df = df.rename(columns={'ts_code': colunms_name[0], 'open': colunms_name[1], 'close': colunms_name[2]})
    dfs = new_df[~ new_df['code'].str.contains('^300|^688|^900')]
    df_a = dfs.loc[(dfs["now"] < 120), colunms_name]
    return df_a


def main(date, s_table, cur_t):
    sql = f"select * from {s_table} where create_date = '{date}'"
    df = pd.read_sql_query(sql, con=engine)
    print(df.head())
    df.drop(['ogc'], axis=1, inplace=True)
    if len(df) == 0:
        return
    dfs = get_stocks()
    print(dfs.head())
    if len(dfs) > 0:
        change = f'c_{cur_t}'
        df.drop(['open'], axis=1, inplace=True)
        new_df = pd.merge(df, dfs, how='inner', left_on=['code'], right_on=['code'])
        print(new_df.head())
        try:
            new_df[change] = (new_df['now'] - new_df['open']) / new_df['open'] * 100
        except:
            new_df[change] = 0
        new_df['ogc'] = (new_df['open'] - new_df['close']) / new_df['close'] * 100
        df_a = new_df.sort_values(by=['ogc'], ascending=True).set_index('create_date').round({change: 2, 'ogc': 2})
        df_a.drop(['now'], axis=1, inplace=True)

        print(df_a.head())

        try:
            engine.execute(f"delete from {s_table} where create_date = '{date}'")
            trans.commit()
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
        # df = ts_data.trade_cal(exchange='', start_date=start_date.strftime(d_format),
        #                        end_date=end_date.strftime(d_format), is_open='1')
        # last_d = df.tail(1)['cal_date'].to_list()[0]
        last_d = '20210202'
        next_d = '20210203'
        cur_date = datetime.strptime(last_d, d_format)
        last_date = cur_date.strftime(date_format)
        print(last_date)
        # 创建连接引擎
        engine = create_engine(f'sqlite:///{db}/{db}.db', echo=False, encoding='utf-8')
        conn = engine.connect()
        trans = conn.begin()
        s_table = f'zjlx'
        t_list_am = [datetime.strftime(x, t_format) for x in
                     pd.date_range(f'{cur_date} 09:30', f'{cur_date} 11:30', freq='30min')]
        t_list_pm = [datetime.strftime(x, t_format) for x in
                     pd.date_range(f'{cur_date} 13:30', f'{cur_date} 14:40', freq='30min')]
        t_list = t_list_am + t_list_pm
        if dd.hour > 15:
            cur_t = '1630'
            t_list.append(cur_t)
        cur_t = '1630'
        if cur_t in t_list:
            main(last_date, s_table, cur_t)

