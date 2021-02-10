# -*- coding: utf-8 -*-
# author: itimor
# 东方财富资金流向，并根据策略筛选股票

from datetime import datetime, timedelta
import tushare as ts

ts_data = ts.pro_api('d256364e28603e69dc6362aefb8eab76613b704035ee97b555ac79ab')
df = ts_data.trade_cal(exchange='', start_date='20210101', end_date='20210131', is_open='1')
print(df['cal_date'].to_list())
