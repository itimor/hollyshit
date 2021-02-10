# -*- coding: utf-8 -*-
# author: itimor
# 东方财富资金流向，并根据策略筛选股票

from datetime import datetime, timedelta
from sqlalchemy import create_engine
import pandas as pd
import tushare as ts
import os


def main(date):
    df = ts_data.daily(trade_date=date)
    if len(df) == 0:
        return
    columns = ['trade_date', 'code', 'open', 'close', 'return', 'high', 'low', 'pre_close', 'vol', 'amount']
    dfs = df.rename(columns={'ts_code': columns[1], 'pct_chg': columns[4]})
    dfs = dfs[~ dfs['code'].str.contains('^200|^300|^688|^900|^N|^C')]
    # dfs['trade_date'] = pd.to_datetime(date, format=d_format)
    # dfs['trade_date'] = dfs['trade_date'].apply(lambda x: x.strftime(d_format))
    new_df = dfs.loc[
        (dfs["close"] < 101), columns]
    # 今日低开幅度
    new_df['ogc'] = (new_df['open'] - new_df['pre_close']) / new_df['pre_close'] * 100
    new_df['vol'] = new_df['vol'] / 1000
    new_df['amount'] = new_df['amount'] / 1000
    for t in t_list:
        change = f'c_{t}'
        new_df[[change]] = 0.0
    new_df[['ogc_x']] = 0.0
    last_df = new_df.set_index('trade_date').round({'vol': 2, 'amount': 2, 'return': 2, 'ogc': 2})
    print(last_df.head())
    last_df.to_sql(s_table, con=engine, index=True, if_exists='append')


if __name__ == '__main__':
    db = 'aaa'
    if not os.path.exists(db):
        os.makedirs(db)
    date_format = '%Y-%m-%d'
    d_format = '%Y%m%d'
    t_format = '%H%M'
    # 获得当天
    dt = '2021-02-19'
    dd = datetime.strptime(dt, date_format)
    dd = datetime.now()
    cur_date = dd.strftime(date_format)
    cur_d = dd.strftime(d_format)
    cur_t = dd.strftime(t_format)
    print(f'今天日期 {cur_d}')
    start_date = dd - timedelta(days=31)
    end_date = dd - timedelta(days=1)
    # t_list生成
    t_list_am = [datetime.strftime(x, t_format) for x in
                 pd.date_range(f'{cur_date} 09:30', f'{cur_date} 11:30', freq='30min')]
    t_list_pm = [datetime.strftime(x, t_format) for x in
                 pd.date_range(f'{cur_date} 13:30', f'{cur_date} 14:40', freq='30min')]
    t_list = t_list_am + t_list_pm
    t_list.append('1630')
    # ts初始化
    ts_data = ts.pro_api('d256364e28603e69dc6362aefb8eab76613b704035ee97b555ac79ab')
    ts_df = ts_data.trade_cal(exchange='', start_date='20210201', end_date=cur_d, is_open='1')
    trade_days = ts_df['cal_date'].to_list()
    # 创建连接引擎
    engine = create_engine(f'sqlite:///{db}/{db}.db', echo=False, encoding='utf-8')
    s_table = 'tsdata'
    for trade_day in trade_days:
        print(f'交易日期 {trade_day}')
        main(trade_day)
