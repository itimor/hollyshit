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


def main():
    sql = f"select * from {s_table} where isST=0 and date > '{s_date}' order by amount desc"
    date_df = pd.read_sql_query(sql, con=engine)
    last_df = date_df[
        (date_df['open'] == date_df['close']) &
        (date_df['close'] == date_df['high']) &
        # (date_df['high'] == date_df['low']) &
        (date_df['pctChg'] > 9) &
        (date_df['close'] > 2) &
        (date_df['close'] < 50)
        ]
    print(last_df)
    last_df.to_sql('stock_yzzt', engine, if_exists='replace', index=False)


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
    s_table = 'stock'
    main()
