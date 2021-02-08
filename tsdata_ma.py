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


def get_stocks():
    colunms_name = ['code', 'open', 'now']
    df = ts_data.daily(ts_code='', start_date=next_d, end_date=next_d)
    new_df = df.rename(columns={'ts_code': colunms_name[0], 'open': colunms_name[1], 'close': colunms_name[2]})
    dfs = new_df[~ new_df['code'].str.contains('^300|^688|^900')]
    df_a = dfs.loc[(dfs["now"] < 120), colunms_name]
    return df_a


def main(date, s_table):
    sql = f"select * from {s_table} where create_date = '{date}'"
    df = pd.read_sql_query(sql, con=engine)
    print(df.head())
    df.drop(['ogc'], axis=1, inplace=True)
    if len(df) == 0:
        return
    dfs = get_stocks()


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
    # ts初始化
    ts_data = ts.pro_api('d256364e28603e69dc6362aefb8eab76613b704035ee97b555ac79ab')
    df = ts_data.trade_cal(exchange='', start_date=start_date.strftime(d_format),
                           end_date=end_date.strftime(d_format), is_open='1')
    last_d = df.tail(1)['cal_date'].to_list()[0]
    cur_date = datetime.strptime(last_d, d_format)
    last_date = cur_date.strftime(date_format)
    print(last_date)
    # 创建连接引擎
    engine = create_engine(f'sqlite:///{db}/{db}.db', echo=False, encoding='utf-8')
    conn = engine.connect()
    trans = conn.begin()
    s_table = 'zjlx'
    main(last_date, s_table)
