# -*- coding: utf-8 -*-
# author: itimor
# 东方财富龙虎榜,并根据策略筛选股票，并发送到tg频道

from datetime import datetime, timedelta
from fake_useragent import UserAgent
import pandas as pd
import numpy as np
import re
import requests
import math

ua = UserAgent()
headers = {'User-Agent': ua.random}


def daterange(start, end, step=1, format="%Y-%m-%d"):
    strptime, strftime = datetime.strptime, datetime.strftime
    days = (strptime(end, format) - strptime(start, format)).days
    return [strftime(strptime(start, format) + timedelta(i), format) for i in range(0, days + 1, step)]


def get_lhb_stocks(begin_date, end_date):
    timeline = daterange(begin_date, end_date)
    dfs = pd.DataFrame()
    for date_id in timeline:
        url = r'http://data.eastmoney.com/DataCenter_V3/stock2016/TradeDetail/pagesize=200,page=1,sortRule=-1,sortType=,startDate=' + date_id + ',endDate=' + date_id + ',gpfw=0,js=var%20data_tab_1.html?rt=26442172'
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
            dfs = dfs.append(df2)
    return dfs


def main(begin_date, end_date, tactics='df1'):
    dfs = get_lhb_stocks(begin_date, end_date)
    timeline = np.unique(dfs['Date'])
    display_column = ['Wind_Code', 'Name', 'Type', 'Close', 'Turn']
    for date in timeline:
        print(date)
        df_a = pd.DataFrame()
        chat_id = ""
        if tactics == 'df1':
            # 策略1：净买额占比从大到小，非科创，游资/机构操作成功率大于45， 股价小于50，净买额占比小于15，大于-1
            df_a = dfs.loc[
                (dfs["Close"] < 50) &
                (dfs["Jm"] > -1) &
                (dfs["Jm"] < 15) &
                (dfs["Type"] != "KC") &
                (dfs["obj_lv"] >= 45) &
                (dfs["obj"] == "主力") &
                (dfs["Date"] == date), display_column
            ].round({'Market': 2}).sort_values(['Close'], ascending=[1])
            chat_id = "@hollystock"
        df = df_a.drop_duplicates('Wind_Code', 'first', inplace=False).reset_index(drop=True)[:5]
        b = [1 / math.log(i + 2) for i in range(0, len(df))]
        df['Buy'] = [i / sum(b) for i in b]
        df[['Close']] = df[['Close']].astype(float)
        df['BuyCount'] = df['Buy'] / df['Close']
        last_df = df.round({'Buy': 2}).to_string(header=None)
        print(last_df)


if __name__ == '__main__':
    t = 16
    date_format = '%Y-%m-%d'

    # 获得当天
    dd = datetime.now()
    if dd.hour > t:
        cur_date = dd.strftime(date_format)
    else:
        cur_date = (dd - timedelta(1)).strftime(date_format)

    # begin_date = '2020-12-01'
    # end_date = '2020-12-30'
    begin_date = cur_date
    end_date = cur_date
    tactics = 'df1'
    main(begin_date, end_date, tactics)
