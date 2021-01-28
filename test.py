# -*- coding: utf-8 -*-
# author: itimor
# 东方财富沪深a股实时行情

from datetime import datetime, timedelta
from fake_useragent import UserAgent
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import tushare as ts
import requests
import re

s_db = 'aaa'
d_db = 'bbb'
date_format = '%Y-%m-%d'
d_format = '%Y%m%d'
ua = UserAgent()
headers = {'User-Agent': ua.random}
s_engine = create_engine(f'sqlite:///bbb/{s_db}.db', echo=False, encoding='utf-8')
s_table = f'zjlx'
cur_d = '20210128'
df = pd.read_sql_query(f'select * from {s_table}', con=s_engine)
df['create_date'] = pd.to_datetime(cur_d, format=d_format)
df['create_date'] = df['create_date'].apply(lambda x: x.strftime(date_format))
df[['open']] = 0.0
t_list = ['0930', '1030', '1430', '1630']
for cur_t in t_list:
    change = f'change_{cur_t}'
    ogc = f'ogc_{cur_t}'
    df[[change]] = 0.0
    df[[ogc]] = 0.0
last_df = df.set_index('create_date')
print(df)
print(df.dtypes)
d_engine = create_engine(f'sqlite:///bbb/{d_db}.db', echo=False, encoding='utf-8')
last_df.to_sql(s_table, con=d_engine, index=True, if_exists='replace')
