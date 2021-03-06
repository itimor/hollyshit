# -*- coding: utf-8 -*-
# author: itimor
# 东方财富资金流向，并根据策略筛选股票

from datetime import datetime, timedelta
from fake_useragent import UserAgent
from sqlalchemy import create_engine
import pandas as pd
import tushare as ts
import requests
import re
import json

ua = UserAgent()
# 今日尾盘入  明日大涨 后天大大涨
"""
SELECT * from b_new where ogc <-2 and master > 10 and change < 6 ORDER by master;
"""
# 今日早盘入  明日大涨 后天大大涨
"""
SELECT * from b_new where ogc <0 and master < 20 and big > 0 and small < 0 and close < 10 ORDER by ogc;
SELECT * from b_new_0930 where abs(ogc) <1 AND master < 10 AND master > 4 AND super < 12 AND super > 0 AND big < 0 AND small < 0 AND mid < 0 AND change < 5 ORDER by master;
"""


# 获取实时行情-网易接口
def get_stocks_by_126(codes):
    limit = 18  # 要发
    code_list = []
    for pre_code in codes:
        c = pre_code.split('.')
        if c[1] == 'SH':
            pre = '0'
        else:
            pre = '1'
        code = f'{pre}{c[0]}'
        code_list.append(code)
    n = int(len(codes) / limit) + 1
    m = 0
    dfs = []
    for i in range(n):
        a = code_list[m:m + limit]
        print(a)
        if len(a) > 0:
            m = (i + 1) * limit
            url = f"http://api.money.126.net/data/feed/{','.join(a)}"
            headers = {'User-Agent': ua.random}
            r = requests.get(url, headers=headers).text
            X = re.split('[)];', r)
            X = re.split('_ntes_quote_callback[(]', X[0])
            b = json.loads(X[1])
            for d in b.values():
                Y = d['code']
                if Y[:1] == 0:
                    pre = 'SH'
                else:
                    pre = 'SZ'
                pre_code = f'{Y[1:]}.{pre}'
                if d['status'] == 0:
                    d_data = {
                        'code': pre_code,
                        'open_x': d['open'],
                        'now': d['price'],
                    }
                else:
                    d_data = {
                        'code': pre_code,
                        'open_x': d['yestclose'],
                        'now': d['yestclose'],
                    }
                dfs.append(d_data)
    dfs_json = json.dumps(dfs)
    df_a = pd.read_json(dfs_json, orient='records')
    return df_a


def main(date, s_table, cur_t):
    sql = f"select * from {s_table} where trade_date = '{date}'"
    df = pd.read_sql_query(sql, con=engine)
    print(df.head())
    df.drop(['ogc'], axis=1, inplace=True)
    if len(df) == 0:
        return
    dfs = get_stocks_by_126(df['code'].to_list())
    print(dfs.head())
    if len(dfs) > 0:
        change = f'c_{cur_t}'
        if cur_t == '0930':
            df.drop(['open'], axis=1, inplace=True)
        else:
            dfs.drop(['open'], axis=1, inplace=True)
        new_df = pd.merge(df, dfs, how='inner', left_on=['code'], right_on=['code'])
        if len(new_df) == 0:
            return
        print(new_df.head())
        try:
            new_df[change] = (new_df['now'] - new_df['open_x']) / new_df['open_x'] * 100
        except:
            new_df[change] = 0
        new_df['ogc'] = (new_df['open_x'] - new_df['close']) / new_df['close'] * 100
        df_a = new_df.sort_values(by=['ogc'], ascending=True).set_index('trade_date').round({change: 2, 'ogc': 2})
        df_a.drop(['now'], axis=1, inplace=True)

        print(df_a.head())

        try:
            conn.execute(f"delete from {s_table} where trade_date = '{date}' and code in {tuple(df_a['code'].to_list())}")
            trans.commit()
            df_a.to_sql(s_table, engine, if_exists='append', index=True)
        except:
            trans.rollback()
            raise

        if cur_t == '0930':
            columns = ['code', 'name', 'return', 'open', 'ogc', change]
            df_b = new_df.loc[
                (new_df[change] > 0.95) &
                (new_df[change] < 1.02) &
                (new_df['ogc'] < 1) &
                (new_df[change] < 3)
                , columns].sort_values(by=['return'], ascending=False)
            if len(df_b) > 0:
                last_df = df_b[:5].to_string(header=None)
                print(last_df)
                chat_id = "@hollystock"
                text = '%s 涨幅小于1大于0.95，高开小于1\n' % date + last_df
                # send_tg(text, chat_id)


if __name__ == '__main__':
    db = 'aaa'
    date_format = '%Y-%m-%d'
    d_format = '%Y%m%d'
    t_format = '%H%M'
    # 获得当天
    dd = datetime.now()
    start_date = dd - timedelta(days=10)
    end_date = dd - timedelta(days=1)
    cur_t = dd.strftime(t_format)
    if dd.hour > 8:
        # ts初始化
        ts.set_token('d256364e28603e69dc6362aefb8eab76613b704035ee97b555ac79ab')
        ts_data = ts.pro_api()
        df_ts = ts_data.trade_cal(exchange='', start_date=start_date.strftime(d_format),
                               end_date=end_date.strftime(d_format), is_open='1')
        last_d = df_ts.tail(1)['cal_date'].to_list()[0]
        cur_date = datetime.strptime(last_d, d_format)
        last_date = cur_date.strftime(date_format)
        print(last_date)
        # 创建连接引擎
        engine = create_engine(f'sqlite:///{db}/{db}.db', echo=False, encoding='utf-8')
        conn = engine.connect()
        trans = conn.begin()
        s_table = f'tsdata'
        t_list_am = [datetime.strftime(x, t_format) for x in
                     pd.date_range(f'{cur_date} 09:30', f'{cur_date} 11:30', freq='30min')]
        t_list_pm = [datetime.strftime(x, t_format) for x in
                     pd.date_range(f'{cur_date} 13:30', f'{cur_date} 14:40', freq='30min')]
        t_list = t_list_am + t_list_pm
        if dd.hour > 15:
            cur_t = '1630'
            t_list.append(cur_t)
        if dd.hour == 9:
            cur_t = '0930'
        if cur_t in t_list:
            main(last_d, s_table, cur_t)
