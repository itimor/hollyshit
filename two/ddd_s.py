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


def handle(df):
    ma_list = [5, 10, 20, 30, 40, 50, 60]
    for ma_len in ma_list:
        # 日涨幅
        day = 5
        df['ma_' + str(ma_len) + '_slop_' + str(day)] = (df['ma' + str(ma_len)] - df['ma' + str(ma_len)].shift(day)) / \
                                                        df['ma' + str(ma_len)].shift(day)
        day = 10
        df['ma_' + str(ma_len) + '_slop_' + str(day)] = (df['ma' + str(ma_len)] - df['ma' + str(ma_len)].shift(day)) / \
                                                        df['ma' + str(ma_len)].shift(day)
    df['highest_10'] = df['high'].rolling(57).max()  # 约定时间内的最高点
    df['is_highest'] = df['close'] == df['highest_10']  # 当日收盘时约定时间内最高点
    df['ma_5_10'] = df['ma5'] - df['ma10']
    df['ma_10_20'] = df['ma10'] - df['ma20']
    df['ma_5_60'] = df['ma5'] - df['ma60']
    df['ma_10_60'] = df['ma10'] - df['ma60']
    df['ma_20_60'] = df['ma20'] - df['ma60']

    # n天涨幅
    df['return_0'] = df['pctChg']
    df['return_1'] = df['pctChg'].shift(1)
    df['return_3'] = df['pctChg'].rolling(3).sum()
    df['return_5'] = df['pctChg'].rolling(5).sum()

    # 上下影线
    df['color'] = df['close'] - df['open']
    df['up_line'] = abs(df['high'] - df['close']) / (df['close'] - df['open'])
    return df


def main():
    sql = f"select * from {s_table} where date > '2021-03-10' and turn > 10 order by turn desc"
    date_df = pd.read_sql_query(sql, con=engine)
    managed_df = date_df.groupby('code').apply(handle).reset_index()
    result_buy = managed_df[
        (managed_df['turn'] > 15) &
        (managed_df['up_line'] > -1) &
        (managed_df['close'] > 3) &
        (managed_df['close'] < 50) &
        (managed_df['hist'] > -1)]
    stock_to_buy = result_buy.groupby('date').apply(lambda df: list(df.code)).reset_index().rename(columns={0: 'stocks'})
    stock_to_buy_dic = stock_to_buy.set_index('date').to_dict()['stocks']

    for dt in stock_to_buy_dic.keys():
        print(dt.split()[0])
        b = [i.split('.')[1] for i in stock_to_buy_dic[dt]]
        print(b)


if __name__ == '__main__':
    db = 'ddd'
    engine = create_engine(f'sqlite:///{db}/{db}.db', echo=False, encoding='utf-8')
    conn = engine.connect()
    trans = conn.begin()
    s_table = 'stock_ma'
    main()
