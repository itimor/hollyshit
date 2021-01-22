# -*- coding: utf-8 -*-
# author: itimor
# 东方财富资金流向，并根据策略筛选股票，并发送到tg频道

from datetime import datetime, timedelta
from fake_useragent import UserAgent
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import tushare as ts
import re
import requests
import os

ua = UserAgent()
headers = {'User-Agent': ua.random}


def get_stocks():
    num = 4000
    url = f'http://push2.eastmoney.com/api/qt/clist/get?pn=1&pz={num}&po=1&np=1&ut=b2884a393a59ad64002292a3e90d46a5&fltt=2&invt=2&fid0=f4001&fid=f184&fs=m:0+t:6+f:!2,m:0+t:13+f:!2,m:0+t:80+f:!2,m:1+t:2+f:!2,m:1+t:23+f:!2,m:0+t:7+f:!2,m:1+t:3+f:!2&stat=1&fields=f12,f14,f2,f3,f184,f69,f75,f81,f87&rt=53664047&cb=jQuery183034292625864656134_1609903402058&_=1609921421171'
    r = requests.get(url, headers=headers).text
    X = re.split('}}', r)[0]
    X = re.split('"diff":', X)[1]
    df = pd.read_json(X, orient='records')
    df.columns = ['close', 'return', 'pre_code', 'name', 'super', 'big', 'mid', 'small', 'master']
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


def main(cur_t):
    dfs = get_stocks()
    columns = ['code', 'name', 'close', 'return', 'master', 'super', 'big', 'mid', 'small']
    if cur_t == '1600':
        df = dfs.loc[
            (dfs["close"] < 50) &
            (dfs["return"] >0 ), columns]
    else:
        df = dfs.loc[
            (dfs["close"] < 50) &
            (dfs["mid"] > 0) &
            (dfs["small"] < 0) &
            (dfs["return"] > -4) &
            (dfs["return"] < 3), columns]
    print(df[:5])
    if len(df) > 0:
        df.to_sql(f'{db}_{cur_t}', con=engine, index=False, if_exists='replace')


if __name__ == '__main__':
    db = 'aaa'
    date_format = '%Y-%m-%d'
    d_format = '%Y%m%d'
    t_format = '%H%M'
    # 获得当天
    dd = datetime.now()
    cur_date = dd.strftime(date_format)
    t_list_am = [datetime.strftime(x, t_format) for x in
                 pd.date_range(f'{cur_date} 09:40', f'{cur_date} 11:30:00', freq='10min')]
    t_list_pm = [datetime.strftime(x, t_format) for x in
                 pd.date_range(f'{cur_date} 13:10', f'{cur_date} 14:50:00', freq='10min')]
    t_list = t_list_am + t_list_pm
    cur_t = dd.strftime(t_format)
    if dd.hour > 15:
        cur_t = '1600'
        t_list.append('1600')
    if cur_t in t_list:
        print(cur_t)
        # ts初始化
        ts_data = ts.pro_api('d256364e28603e69dc6362aefb8eab76613b704035ee97b555ac79ab')
        df = ts_data.trade_cal(exchange='', start_date=dd.strftime(d_format), end_date=dd.strftime(d_format),
                               is_open='1')
        print(df)
        if not os.path.exists(cur_date):
            os.makedirs(cur_date)

        # 创建连接引擎
        engine = create_engine(f'sqlite:///{cur_date}/{db}.db', echo=False, encoding='utf-8')
        if len(df) > 0:
            main(cur_t)
