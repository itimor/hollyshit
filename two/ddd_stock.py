# -*- coding: utf-8 -*-
# author: itimor
# 连涨后回调，优先选择换手率高的 > 19%

from datetime import datetime, timedelta
from sqlalchemy import create_engine
import pandas as pd
import tushare as ts
import baostock as bs

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
    for code in stock_df["code"]:
        print(code)
        k_rs = bs.query_history_k_data_plus(code,
                                            "date,code,open,high,low,close,preclose,volume,amount,turn,pctChg,isST",
                                            start_date, end_date)
        data_df = data_df.append(k_rs.get_data())
    print(data_df.head())
    data_df.to_sql(s_table, engine, if_exists='replace', index=False)


if __name__ == '__main__':
    db = 'ddd'
    if not os.path.exists(db):
        os.makedirs(db)
    date_format = '%Y-%m-%d'
    d_format = '%Y%m%d'
    t_format = '%H%M'
    # 获得当天
    dd = datetime.now()
    start_date = dd - timedelta(days=20)
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
    trade_days = df_ts.tail(10)['cal_date'].to_list()
    # 创建连接引擎
    engine = create_engine(f'sqlite:///{db}/{db}.db', echo=False, encoding='utf-8')
    conn = engine.connect()
    trans = conn.begin()
    s_table = 'stock'
    #### 登陆系统 ####
    lg = bs.login()
    s_date = datetime.strptime(trade_days[0], d_format)
    e_date = datetime.strptime(trade_days[-1], d_format)
    main(s_date.strftime(date_format), e_date.strftime(date_format))
    #### 登出系统 ####
    bs.logout()
