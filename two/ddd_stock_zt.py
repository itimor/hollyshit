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
    sql = f"select * from {s_table} where date == '{end_date} 00:00:00.000000' and isST=0 and pctChg>9.7 order by date asc"
    df_code = pd.read_sql_query(sql, con=engine)
    columns = ['date', 'time', 'code', 'open', 'high', 'low', 'close']
    # for code in ['sz.003039']:
    a = []
    for code in df_code['code']:
        print(code)
        df_code_close = df_code.loc[df_code.code == code]
        k_rs = bs.query_history_k_data_plus(code, ','.join(columns), start_date, end_date,
                                            frequency="5", adjustflag="3")
        df_k = k_rs.get_data()
        round_dict = {}
        for column in columns[3:]:
            round_dict[column] = 2
        df_k[columns[3:]] = df_k[columns[3:]].apply(pd.to_numeric, errors='coerce').fillna(0.0)
        df_k = df_k.reset_index(drop=True).round(round_dict)
        df_k_all = df_k.loc[df_k['close'] == df_code_close['close'].to_list()[0]]
        df_k_all = df_k_all.reset_index(drop=True)
        df_code_close['time'] = df_k_all.loc[0]['time'][8:12]
        a.append(df_code_close)
    data_df = pd.concat(a, ignore_index=True)
    last_df = data_df.sort_values(['time'], ascending=[1])
    print(last_df)
    last_df.to_sql('stock_zt', engine, if_exists='append', index=False)


if __name__ == '__main__':
    db = 'ddd'
    if not os.path.exists(db):
        os.makedirs(db)
    date_format = '%Y-%m-%d'
    d_format = '%Y%m%d'
    t_format = '%H%M'
    # 获得当天
    dd = datetime.now()
    start_date = dd - timedelta(days=7)
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
    trade_days = df_ts.tail(61)['cal_date'].to_list()
    # 创建连接引擎
    engine = create_engine(f'sqlite:///{db}/{db}.db', echo=False, encoding='utf-8')
    conn = engine.connect()
    trans = conn.begin()
    s_table = 'stock'
    #### 登陆系统 ####
    lg = bs.login()
    s_date = datetime.strptime(trade_days[-2], d_format)
    e_date = datetime.strptime(trade_days[-2], d_format)
    main(s_date.strftime(date_format), e_date.strftime(date_format))
    #### 登出系统 ####
    bs.logout()
    end_time = datetime.now()
    spent_time = int((end_time - dd).seconds / 60)
    print(f'spent time {spent_time} min')
