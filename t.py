from datetime import datetime, timedelta
from fake_useragent import UserAgent
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import tushare as ts
import re
import requests
import os

db = 'aaa'
date_format = '%Y-%m-%d'
d_format = '%Y%m%d'
t_format = '%H%M'
# 获得当天
dd = datetime.now()
cur_date = dd.strftime(date_format)

t_list_am = [datetime.strftime(x, t_format) for x in
             pd.date_range(f'{cur_date} 09:30', f'{cur_date} 11:30', freq='30min')]
t_list_pm = [datetime.strftime(x, t_format) for x in
             pd.date_range(f'{cur_date} 13:30', f'{cur_date} 14:40', freq='30min')]
t_list = t_list_am + t_list_pm
t_list.append('1630')
print(t_list)
