# -*- coding: utf-8 -*-
# author: itimor
# 东方财富资金流向，并根据策略筛选股票

from datetime import datetime, timedelta
from sqlalchemy import create_engine
import pandas as pd
import tushare as ts
import baostock as bs
import os


def get_stock_name():
    df = ts_data.namechange(ts_code='', fields='ts_code,name')
    df.drop_duplicates(subset=['ts_code'], keep='first', inplace=True)
    return df


def main(date):
    dfs = ts_data.daily(trade_date=date)
    if len(dfs) == 0:
        return
    df = dfs[~ dfs['ts_code'].str.contains('^200|^300|^688|^900|^N|^C')]
    df_name = get_stock_name()
    df_merge = pd.merge(df, df_name, how='inner', left_on=['ts_code'], right_on=['ts_code'])
    df_st = df_merge[~ df_merge['name'].str.contains('ST')]
    last_df = df_st.set_index('trade_date').round({'vol': 2, 'amount': 2})
    print(last_df.head())
    ma_list = [5, 10, 20, 30, 40, 50, 60]
    for ma in ma_list:
        last_df['ma' + str(ma)] = 0.0
    last_df['hist'] = 0.0
    last_df.to_sql(s_table, con=engine, index=True, if_exists='append')


if __name__ == '__main__':
    db = 'ddd'
    if not os.path.exists(db):
        os.makedirs(db)
    date_format = '%Y-%m-%d'
    d_format = '%Y%m%d'
    t_format = '%H%M'
    dd = datetime.now()
    cur_date = dd.strftime(date_format)
    cur_d = dd.strftime(d_format)
    cur_t = dd.strftime(t_format)
    print(f'今天日期 {cur_d}')
    # ts初始化
    ts.set_token('d256364e28603e69dc6362aefb8eab76613b704035ee97b555ac79ab')
    ts_data = ts.pro_api()
    df_ts = ts_data.trade_cal(exchange='', start_date=cur_d, end_date=cur_d, is_open='1')
    trade_days = df_ts['cal_date'].to_list()
    # 创建连接引擎
    engine = create_engine(f'sqlite:///{db}/{db}.db', echo=False, encoding='utf-8')
    s_table = 'stock'
    for trade_day in trade_days:
        print(f'交易日期 {trade_day}')
        main(trade_day)
