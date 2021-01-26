# -*- coding: utf-8 -*-
# author: itimor
# 东方财富机构调研

from datetime import datetime, timedelta
from fake_useragent import UserAgent
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import tushare as ts
import re
import requests
import os
import json

ua = UserAgent()
headers = {'User-Agent': ua.random}


def get_stocks():
    num = 288
    url = f'http://datainterface3.eastmoney.com/EM_DataCenter_V3/api/JGDYHZ/GetJGDYMX?js=datatable2799830&tkn=eastmoney&secuCode=&sortfield=0&sortdirec=1&pageNum=1&pageSize={num}&cfg=jgdyhz&p=16&pageNo=16&_=1611576606243'
    r = requests.get(url, headers=headers).text
    X = re.split('}]}', r)[0]
    X = re.split('Close","Data":', X)[1]
    s_data = eval(X)
    s_dict = dict()
    for s in s_data:
        a = s.split('|')
        if a[5] not in s_dict:
            s_dict[a[5]] = {
                'pre_code': a[5],
                'name': a[6],
                'close': float(a[17]),
                'return': float(a[18]),
                'num': int(a[4]),
                'sum': int(a[4]),
                'start_day': a[7],
                'end_day': a[7],
            }
        else:
            s_dict[a[5]]['sum'] += int(a[4])
            s_dict[a[5]]['start_day'] = a[7]

    df = pd.DataFrame(s_dict.values())
    df['code'] = str(df['pre_code'])
    s_codes = []
    for i in df['pre_code']:
        if len(str(i)) < 6:
            s = '0' * (6 - len(str(i))) + str(i)
        else:
            s = str(i)
        if s[0] == '6':
            s = s + '.SH'
        else:
            s = s + '.SZ'
        if len(s_codes) == 0:
            s_codes = [s]
        else:
            s_codes.append(s)
    df['code'] = s_codes
    df.drop(['pre_code'], axis=1, inplace=True)
    dfs = df[~ df['name'].str.contains('ST')]
    last_dfs = dfs[~ dfs['code'].str.contains('^300|^688|^900')]
    return last_dfs


def main():
    dfs = get_stocks()
    columns = ['code', 'name', 'close', 'return', 'num', 'sum', 'start_day', 'end_day']
    table = f'b_new'
    df = dfs.loc[
        (dfs["sum"] > 10) &
        (dfs["close"] < 50), columns]
    print(df[:5])
    df.to_sql(table, con=engine, index=False, if_exists='replace')


if __name__ == '__main__':
    db = 'ddd'
    d_format = '%Y%m%d'
    t_format = '%H%M'
    # 获得当天
    dd = datetime.now()
    cur_d = dd.strftime(d_format)
    cur_t = dd.strftime(t_format)
    if dd.hour > 15:
        # ts初始化
        ts_data = ts.pro_api('d256364e28603e69dc6362aefb8eab76613b704035ee97b555ac79ab')
        df = ts_data.trade_cal(exchange='', start_date=cur_d, end_date=cur_d, is_open='1')
        print(df)
        if not os.path.exists(cur_d):
            os.makedirs(cur_d)
        if len(df) > 0:
            # 创建连接引擎
            engine = create_engine(f'sqlite:///{cur_d}/{db}.db', echo=False, encoding='utf-8')
            main()
