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

ua = UserAgent()
headers = {'User-Agent': ua.random}


ts_data = ts.pro_api('d256364e28603e69dc6362aefb8eab76613b704035ee97b555ac79ab')
df = ts_data.daily(ts_code='', start_date='20210120', end_date='20210120')
print(df)
