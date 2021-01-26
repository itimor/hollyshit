# -*- coding: utf-8 -*-
# author: itimor
# 东方财富资金流向

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
    timestamp = datetime.timestamp(dd)
    t1 = int(timestamp * 1000)
    num = 1618
    df_list = list()
    for n in range(1, 4):
        print(n)
        url = f'http://push2.eastmoney.com/api/qt/clist/get?cb=jQuery1123008677483930340002_1611572690331&fid=f184&po=1&pz={num}&pn={n}&np=1&fltt=2&invt=2&ut=b2884a393a59ad64002292a3e90d46a5&fs=m%3A0%2Bt%3A6%2Bf%3A!2%2Cm%3A0%2Bt%3A13%2Bf%3A!2%2Cm%3A0%2Bt%3A80%2Bf%3A!2%2Cm%3A1%2Bt%3A2%2Bf%3A!2%2Cm%3A1%2Bt%3A23%2Bf%3A!2%2Cm%3A0%2Bt%3A7%2Bf%3A!2%2Cm%3A1%2Bt%3A3%2Bf%3A!2&fields=f12%2Cf14%2Cf2%2Cf3%2Cf62%2Cf184%2Cf66%2Cf69%2Cf72%2Cf75%2Cf78%2Cf81%2Cf84%2Cf87%2Cf204%2Cf205%2Cf124'
        r = requests.get(url, headers=headers).text
        X = re.split('}}', r)[0]
        X = re.split('"diff":', X)[1]
        df_n = pd.read_json(X, orient='records')
        df_list.append(df_n)
    dfs = pd.concat(df_list)
    df = dfs[['f12', 'f14', 'f2', 'f3', 'f184', 'f69', 'f75', 'f81', 'f87']]
    df.columns = ['pre_code', 'name', 'close', 'return', 'master', 'super', 'big', 'mid', 'small']
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
    df_st = df[~ df['name'].str.contains('ST')]
    df_close_null = df_st.loc[df_st['close'] != '-']
    last_df = df_close_null[~ df_close_null['code'].str.contains('^300|^688|^900')]
    print(last_df)
    return last_df


def main():
    dfs = get_stocks()
    columns = ['code', 'name', 'close', 'return', 'master', 'super', 'big', 'mid', 'small']
    table = f'all_zjlx'
    df = dfs.loc[
        (dfs["close"] < 100), columns]
    print(df[:5])
    df.to_sql(table, con=engine, index=False, if_exists='replace')


if __name__ == '__main__':
    db = 'bbb'
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
