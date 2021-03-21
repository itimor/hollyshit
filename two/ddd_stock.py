# -*- coding: utf-8 -*-
# author: itimor
# 连涨后回调，优先选择换手率高的 > 19%

from datetime import datetime, timedelta
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import tushare as ts
import baostock as bs

import talib
import os

# 设置最大列数，避免只显示部分列
pd.set_option('display.max_columns', 1000)
# 设置最大行数，避免只显示部分行数据
pd.set_option('display.max_rows', 1000)
# 设置显示宽度
pd.set_option('display.width', 1000)
# 设置每列最大宽度，避免属性值或列名显示不全
pd.set_option('display.max_colwidth', 1000)


def main(start_date, end_date):
    stock_rs = bs.query_all_stock(end_date)
    stock_df = stock_rs.get_data()
    data_df = pd.DataFrame()
    columns = ['date', 'code', 'open', 'high', 'low', 'close', 'preclose', 'volume', 'amount', 'turn', 'pctChg', 'isST']
    n = 0
    # for code in stock_df["code"][:250]:
    for code in stock_df["code"]:
        n += 1
        print(n)
        if code[:6] in ['sh.688', 'sz.200', 'sz.300', 'sz.400', 'sz.900', 'sz.399', 'sh.000']:
            continue
        print(code)
        k_rs = bs.query_history_k_data_plus(code, ','.join(columns), start_date, end_date,
                                            frequency="d", adjustflag="3")
        df_code = k_rs.get_data()
        df_code[columns[2:-1]] = df_code[columns[2:-1]].apply(pd.to_numeric, errors='coerce').fillna(0.0)
        data_df = data_df.append(df_code)
    data_df['date'] = data_df['date'].apply(pd.to_datetime, format=date_format)
    round_dict = {}
    data_df['volume'] = data_df['volume'] / 10000
    data_df['amount'] = data_df['amount'] / 10000
    for column in columns[2:-1]:
        round_dict[column] = 2
    df_merge = pd.merge(data_df, stock_df, how='inner', left_on=['code'], right_on=['code'])
    last_df = df_merge.loc[df_merge.close < 80].reset_index(drop=True).round(round_dict)
    print(last_df.head())
    # if typ == 'today':
    #     last_df.to_sql(s_table, engine, if_exists='append', index=False)
    # else:
    #     last_df.to_sql(s_table, engine, if_exists='replace', index=False)


if __name__ == '__main__':
    db = 'ddd'
    if not os.path.exists(db):
        os.makedirs(db)
    date_format = '%Y-%m-%d'
    d_format = '%Y%m%d'
    t_format = '%H%M'
    # 获得当天
    dd = datetime.now()
    start_date = dd - timedelta(days=120)
    end_date = dd - timedelta(days=1)
    cur_date = dd.strftime(date_format)
    cur_d = dd.strftime(d_format)
    cur_t = dd.strftime(t_format)
    print(f'今天日期 {cur_date}')
    # ts初始化
    ts.set_token('d256364e28603e69dc6362aefb8eab76613b704035ee97b555ac79ab')
    ts_data = ts.pro_api()
    df_ts = ts_data.trade_cal(exchange='', start_date=start_date.strftime(d_format),
                              end_date=dd.strftime(d_format), is_open='1')
    trade_days = df_ts.tail(60)['cal_date'].to_list()
    # 创建连接引擎
    engine = create_engine(f'sqlite:///{db}/{db}.db', echo=False, encoding='utf-8')
    conn = engine.connect()
    trans = conn.begin()
    s_table = 'stock'
    #### 登陆系统 ####
    lg = bs.login()
    typ = 'today'  # all|today
    if typ == 'all':
        s_date = datetime.strptime(trade_days[0], d_format)
        e_date = datetime.strptime(trade_days[-1], d_format)
    else:
        s_date = datetime.strptime(trade_days[-1], d_format)
        e_date = datetime.strptime(trade_days[-1], d_format)
    main(s_date.strftime(date_format), e_date.strftime(date_format))
    #### 登出系统 ####
    bs.logout()
    end_time = datetime.now()
    spent_time = int((end_time - dd).seconds / 60)
    print(f'spent time {spent_time} min')
