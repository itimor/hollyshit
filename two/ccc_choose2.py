# -*- coding: utf-8 -*-
# author: itimor
# 连涨后回调，优先选择换手率高的 > 19%

from datetime import datetime, timedelta
from sqlalchemy import create_engine
import pandas as pd
import tushare as ts

# 设置最大列数，避免只显示部分列
pd.set_option('display.max_columns', 1000)
# 设置最大行数，避免只显示部分行数据
pd.set_option('display.max_rows', 1000)
# 设置显示宽度
pd.set_option('display.width', 1000)
# 设置每列最大宽度，避免属性值或列名显示不全
pd.set_option('display.max_colwidth', 1000)


def handle(df):
    df['highest_10'] = df['high'].rolling(3).max()  # 约定时间内的最高点
    df['is_highest'] = df['close'] == df['highest_10']  # 当日收盘时约定时间内最高点
    df['ma_5_10'] = df['ma5'] - df['ma10']
    df['ma_10_20'] = df['ma10'] - df['ma20']
    df['ma_5_60'] = df['ma5'] - df['ma60']
    df['ma_10_60'] = df['ma10'] - df['ma60']
    df['ma_20_60'] = df['ma20'] - df['ma60']

    # n天涨幅
    df['return_0'] = df['pct_chg']
    df['return_1'] = df['pct_chg'].shift(1)
    df['return_2'] = df['pct_chg'].shift(2)
    df['return_3'] = df['pct_chg'].shift(2)
    df['return_5'] = df['pct_chg'].rolling(5).sum()
    return df


def main():
    sql = f"select * from {s_table} where trade_date >= '{trade_days[0]}' and trade_date <= '{trade_days[59]}' order by trade_date asc"
    df = pd.read_sql_query(sql, con=engine)
    managed_df = df.groupby('ts_code').apply(handle).reset_index()
    result_buy = managed_df[
        (managed_df['ma_5_10'] > 0) &
        # (managed_df['ma_10_20'] > 0) &
        # (managed_df['up_line'] < 0.8) &
        # (managed_df['down_line'] < 0.8)&
        (managed_df['return_0'] > 3) &
        (managed_df['return_1'] > 3) &
        (managed_df['return_2'] < 2) &
        (managed_df['return_3'] < 0) &
        (managed_df['close'] > 2) &
        (managed_df['close'] < 80) &
        (managed_df['hist'] > -2)
        ]

    stock_to_buy = result_buy.groupby('trade_date').apply(lambda df: list(df.ts_code)).reset_index().rename(
        columns={0: 'stocks'})
    stock_to_buy_dic = stock_to_buy.set_index('trade_date').to_dict()['stocks']

    for dt in stock_to_buy_dic.keys():
        print(dt)
        b = [i.split('.')[0] for i in stock_to_buy_dic[dt]]
        print(b)


if __name__ == '__main__':
    db = 'ccc'
    date_format = '%Y-%m-%d'
    d_format = '%Y%m%d'
    t_format = '%H%M'
    # 获得当天
    dd = datetime.now()
    start_date = dd - timedelta(days=100)
    end_date = dd - timedelta(days=1)
    cur_date = dd.strftime(date_format)
    cur_d = dd.strftime(d_format)
    cur_t = dd.strftime(t_format)
    print(f'今天日期 {cur_d}')
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
    s_table = 'ccc_ma'
    main()
