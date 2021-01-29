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

s_db = 'bbb'
d_db = 'bbb'
date_format = '%Y-%m-%d'
d_format = '%Y%m%d'
ua = UserAgent()
headers = {'User-Agent': ua.random}
s_engine = create_engine(f'sqlite:///bbb/{s_db}.db', echo=False, encoding='utf-8')
s_table = f'zjlx'
cur_d = '20210128'
df = pd.read_sql_query(f'select * from {s_table}', con=s_engine)
t_list = ['0930', '1030', '1430', '1630']
round_dict = dict()
for cur_t in t_list:
    change = f'change_{cur_t}'
    ogc = f'ogc_{cur_t}'
    round_dict[change] = 2
    round_dict[ogc] = 2
last_df = df.set_index('create_date').round(round_dict)
print(df)
print(df.dtypes)
d_engine = create_engine(f'sqlite:///bbb/{d_db}.db', echo=False, encoding='utf-8')
last_df.to_sql(s_table, con=d_engine, index=True, if_exists='replace')
