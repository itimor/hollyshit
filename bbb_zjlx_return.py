# -*- coding: utf-8 -*-
# author: itimor
# 东方财富资金流向，并根据策略筛选股票

from datetime import datetime, timedelta
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import tushare as ts


def main(date):
    table = f'{db}_x'
    df = pd.read_sql_query(f'select * from {table}', con=engine)
    codes = ','.join(df['code'].to_list())
    df_a = ts_data.daily(ts_code=codes, start_date=date, end_date=date)
    if len(df_a) > 0:
        df_a_columns = ['ts_code', 'open', 'high', 'low', 'change']
        new_df = pd.merge(df, df_a[df_a_columns], how='left', left_on=['code'], right_on=['ts_code'])
        new_df['ogc'] = new_df['open'] - new_df['close']
        columns = ['code', 'name', 'close', 'return', 'master', 'super', 'big', 'mid', 'small', 'open', 'high', 'low',
                   'change', 'ogc']
        df_b = new_df[columns]
        print(df_b[:5])
        df_b.to_sql(table, con=engine, index=False, if_exists='replace')


if __name__ == '__main__':
    db = 'bbb'
    level = 5
    date_format = '%Y-%m-%d'
    d_format = '%Y%m%d'
    t_format = '%H%M'
    # 获得当天
    dd = datetime.now()
    cur_date = dd.strftime(date_format)
    cur_d = dd.strftime(d_format)
    cur_t = dd.strftime(t_format)
    if dd.hour > 15:
        # ts初始化
        ts_data = ts.pro_api('d256364e28603e69dc6362aefb8eab76613b704035ee97b555ac79ab')
        df = ts_data.trade_cal(exchange='', start_date=cur_d, end_date=cur_d, is_open='1')
        print(df)
        # 创建连接引擎
        engine = create_engine(f'sqlite:///{cur_date}/{db}.db', echo=False, encoding='utf-8')
        main(cur_d)

