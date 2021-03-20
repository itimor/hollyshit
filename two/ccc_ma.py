# -*- coding: utf-8 -*-
# author: itimor
# 东方财富资金流向，并根据策略筛选股票

from datetime import datetime, timedelta
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import tushare as ts
import talib

# 设置最大列数，避免只显示部分列
pd.set_option('display.max_columns', 1000)
# 设置最大行数，避免只显示部分行数据
pd.set_option('display.max_rows', 1000)
# 设置显示宽度
pd.set_option('display.width', 1000)
# 设置每列最大宽度，避免属性值或列名显示不全
pd.set_option('display.max_colwidth', 1000)


def main(date):
    ma_list = [5, 10, 20, 30, 40, 50, 60]
    sql = f"select * from {s_table} where trade_date = '{date}'"
    df = pd.read_sql_query(sql, con=engine)
    code_list = df['ts_code'].to_list()
    # code_list = ['002967.SZ', '002966.SZ']
    last_df = pd.DataFrame()
    for ts_code in code_list:
        print(ts_code)
        sql = f"select * from {s_table} where ts_code = '{ts_code}' and trade_date >= '{trade_days[0]}' order by trade_date asc"
        df_code = pd.read_sql_query(sql, con=engine)
        for ma in ma_list:
            df_code['ma' + str(ma)] = df_code['close'].rolling(ma).mean()
        prices = df_code['close'].map(np.float)
        macd, signal, hist = talib.MACD(np.array(prices), 12, 26, 9)  # 计算macd各个指标
        df_code['hist'] = hist  # macd所在区域
        last_df = pd.concat([df_code, last_df])
    round_dict = {}
    for ma in ma_list:
        round_dict['ma' + str(ma)] = 2
    round_dict['hist'] = 2
    last_df = last_df.reset_index(drop=True).round(round_dict)
    print(last_df.head())
    last_df.to_sql('ccc_ma', engine, if_exists='replace', index=False)


if __name__ == '__main__':
    db = 'ccc'
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
    s_table = 'tsdata'
    main(trade_days[59])
