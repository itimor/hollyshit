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


# 判断是否为一字涨停的函数
def judge_yizizhangting(df):
    if (df['high'] - df['low']) / df['low'] * 100 < 2:
        return True
    else:
        return False


def handle(df):
    df['highest_10'] = df['high'].rolling(57).max()  # 约定时间内的最高点
    df['is_highest'] = df['close'] == df['highest_10']  # 当日收盘时约定时间内最高点

    # n天涨幅
    df['return_0'] = df['pctChg']
    df['return_3'] = df['pctChg'].rolling(3).sum()
    df['hl'] = (df['high'] - df['low'] + 0.0001) / df['low'] * 100

    # 上下影线
    df['color'] = df['close'] - df['open']
    df['yizizhangting'] = df.apply(judge_yizizhangting, axis=1)
    return df


def sort_stock(x, stock_to_buy_dic):
    dt = x
    st = stock_to_buy_dic[x]
    if len(st) == 1:
        code_list = st[0]
        sql = f"select * from {s_table} where date = '{dt}' and code = '{code_list}'"
    else:
        code_list = tuple(st)
        sql = f"select * from {s_table} where date = '{dt}' and code in {code_list}"
    df = pd.read_sql_query(sql, con=engine)
    df['hl'] = (df['high'] - df['low'] + 0.0001) / df['low'] * 100
    tmp = df[['code', 'turn', 'hist', 'pctChg', 'hl', 'code_name']].copy()
    # 按照换手率降序排序、市值升序排序、每股收益降序排序
    tmp.sort_values(by=['turn', 'hist'], ascending=[0, 1], inplace=True)
    print(dt.split()[0])
    print(tmp)
    return tmp


def main():
    sql = f"select * from {s_table} where date > '{s_date}' order by amount desc"
    date_df = pd.read_sql_query(sql, con=engine)
    managed_df = date_df.groupby('code').apply(handle).reset_index()
    result_buy = managed_df[
        #(managed_df['turn'] > 7) &
        (managed_df['open'] == managed_df['close']) &
        (managed_df['open'] == managed_df['high']) &
        (managed_df['high'] == managed_df['low']) &
        (managed_df['close'] > 2) &
        (managed_df['close'] < 50)
        ]
    stock_to_buy = result_buy.groupby('date').apply(lambda df: list(df.code)).reset_index().rename(
        columns={0: 'stocks'})
    stock_to_buy_dic = stock_to_buy.set_index('date').to_dict()['stocks']

    # 选出来的股票还需进行排序
    for i in stock_to_buy_dic.keys():
        sort_stock(i, stock_to_buy_dic)


if __name__ == '__main__':
    db = 'ddd'
    date_format = '%Y-%m-%d'
    d_format = '%Y%m%d'
    t_format = '%H%M'
    # 获得当天
    dd = datetime.now()
    start_date = dd - timedelta(days=10)
    s_date = start_date.strftime(date_format)
    engine = create_engine(f'sqlite:///{db}/{db}.db', echo=False, encoding='utf-8')
    conn = engine.connect()
    trans = conn.begin()
    s_table = 'stock_ma'
    main()
