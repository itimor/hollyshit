# -*- coding: utf-8 -*-
# author: itimor
# 东方财富资金流向，并根据策略筛选股票

from datetime import datetime, timedelta
from sqlalchemy import create_engine
import pandas as pd
import tushare as ts
import os
ts.set_token('d256364e28603e69dc6362aefb8eab76613b704035ee97b555ac79ab')
ts_data = ts.pro_api()

# 获取历史 ma5 ma10 ma20
df = ts.get_hist_data('600848', start='2021-02-09', end='2021-02-09')
print(df)

# def get_stock_name():
#     df = ts_data.namechange(ts_code='', fields='ts_code,name')
#     df = df[~ df['ts_code'].str.contains('^200|^300|^688|^900')]
#     df.drop_duplicates(subset=['ts_code'], keep='first', inplace=True)
#     return df
#
# db = 'aaa'
# engine = create_engine(f'sqlite:///{db}/{db}.db', echo=False, encoding='utf-8')
# s_table = 'tsdata'
# sql = f"select * from {s_table}"
# df = pd.read_sql_query(sql, con=engine)
# df_name = get_stock_name()
# last_df = pd.merge(df, df_name, how='inner', left_on=['code'], right_on=['ts_code'])
# print(last_df)
# last_df.to_sql(s_table, con=engine, index=False, if_exists='replace')
