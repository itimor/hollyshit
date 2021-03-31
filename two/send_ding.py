# -*- coding: utf-8 -*-
# author: itimor
# 东方财富龙虎榜,并根据策略筛选股票，并发送到ding频道

from datetime import datetime, timedelta
from sqlalchemy import create_engine
import pandas as pd
from two.send_msg import to_ding


def main(date):
    columns = ['ts_code', 'name', 'close', 'amount']

    s_table = 'stock_yzzt'
    sql = f"select * from {s_table} where trade_date == '{date}' order by amount desc limit 10"
    df = pd.read_sql_query(sql, con=engine)
    text_df = df[columns].to_string(header=None)
    text1 = 'stock 一字涨停\n' + text_df

    s_table = 'stock_zt'
    sql = f"select * from {s_table} where trade_date == '{date}' and time='0935' order by amount desc limit 10"
    df = pd.read_sql_query(sql, con=engine)
    text_df = df[columns].to_string(header=None)
    text2 = 'stock 急速涨停\n' + text_df

    last_text = text1 + '\n\r' + text2
    to_ding(last_text)


if __name__ == '__main__':
    db = 'ccc'
    date_format = '%Y-%m-%d'
    d_format = '%Y%m%d'
    t_format = '%H%M'
    # 获得当天
    dd = datetime.now()
    # 创建连接引擎
    engine = create_engine(f'sqlite:///{db}/{db}.db', echo=False, encoding='utf-8')
    conn = engine.connect()
    trans = conn.begin()
    d = dd.strftime(d_format)
    # d = '20210330'
    main(d)
