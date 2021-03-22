# -*- coding: utf-8 -*-
# author: itimor
# 东方财富龙虎榜,并根据策略筛选股票，并发送到tg频道

from datetime import datetime, timedelta
from fake_useragent import UserAgent
from sqlalchemy import create_engine
import pandas as pd
import tushare as ts
import numpy as np
import re
import requests
import math

ua = UserAgent()
headers = {'User-Agent': ua.random}


def get_lhb_stocks(start_date, end_date):
    url = f'http://data.eastmoney.com/DataCenter_V3/stock2016/TradeDetail/pagesize=500,page=1,sortRule=-1,sortType=,startDate={start_date},endDate={end_date},gpfw=0,js=var%20data_tab_2.html?rt=26939571'
    r = requests.get(url).text
    X = re.split(',"url"', r)[0]
    X = re.split('"data":', X)[1]
    df = pd.read_json(X, orient='records')
    if len(df) > 0:
        df2 = df[
            ['Tdate', 'SCode', 'SName', 'ClosePrice', 'Chgradio', 'JmRate', 'Dchratio', 'Ltsz', 'JD']]
        colunms = ['Code', 'Name', 'Close', 'Radio', 'Jm', 'Turn']
        df2 = df2.rename(
            columns={'Tdate': 'Date', 'SCode': colunms[0], 'SName': colunms[1],
                     'ClosePrice': colunms[2], 'Chgradio': colunms[3], 'JmRate': colunms[4],
                     'Dchratio': colunms[5]})
        df2['Wind_Code'] = str(df2['Code'])
        df2['Market'] = df2['Ltsz'].map(lambda x: x / 100000000)
        s_codes = []
        s_types = []
        for i in df2['Code']:
            if len(str(i)) < 6:
                s = '0' * (6 - len(str(i))) + str(i)
            else:
                s = str(i)
            if s[0] == '6':
                s_type = 'SH'
            elif s[0] == '3':
                s_type = 'KC'
            else:
                s_type = 'SZ'
            if len(s_codes) == 0:
                s_codes = [s]
                s_types = [s_type]
            else:
                s_codes.append(s)
                s_types.append(s_type)
        df2['Wind_Code'] = s_codes
        df2['Type'] = s_types
        s_obj = []
        s_obj_lv = []
        for i in df2['JD']:
            b = re.findall('(实力游资|机构)(买入|卖出)，成功率(\d+.\d+)%', i)
            if len(b) > 0:
                obj = '主力'
                obj_lv = float(b[0][2])
            else:
                obj = '扑街'
                obj_lv = 0
            s_obj.append(obj)
            s_obj_lv.append(obj_lv)
        df2['obj'] = s_obj
        df2['obj_lv'] = s_obj_lv
    return df2


def main(begin_date, end_date):
    dfs = get_lhb_stocks(begin_date, end_date)
    timeline = np.unique(dfs['Date'])
    display_column = ['Wind_Code', 'Name', 'Type', 'Close', 'Turn']


if __name__ == '__main__':
    db = 'ddd'
    date_format = '%Y-%m-%d'
    d_format = '%Y%m%d'
    t_format = '%H%M'
    dd = datetime.now()
    cur_date = dd.strftime(date_format)
    cur_d = dd.strftime(d_format)
    cur_t = dd.strftime(t_format)
    start_date = dd - timedelta(days=13)
    print(f'今天日期 {cur_d}')
    # ts初始化
    ts.set_token('d256364e28603e69dc6362aefb8eab76613b704035ee97b555ac79ab')
    ts_data = ts.pro_api()
    df_ts = ts_data.trade_cal(exchange='', start_date=start_date.strftime(d_format),
                              end_date=dd.strftime(d_format), is_open='1')
    trade_days = df_ts['cal_date'].to_list()
    s_table = 'lhb'
    s_date = datetime.strptime(trade_days[-3], d_format)
    e_date = datetime.strptime(trade_days[-1], d_format)
    main(s_date, e_date)
