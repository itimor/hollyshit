# -*- coding: utf-8 -*-
# author: itimor
# 东方财富资金流向，并根据策略筛选股票

from datetime import datetime, timedelta
from sqlalchemy import create_engine
import pandas as pd
import tushare as ts


def get_stocks(codes):
    data = {
        'code': codes,
        'ma5': [],
        'ma10': [],
        'ma20': [],
    }
    for code in codes:
        print(code)
        df_code = ts_data.daily(ts_code=code, start_date=start_date.strftime(d_format),
                                end_date=end_date.strftime(d_format))
        for i in [5, 10, 20]:
            dfs = df_code['close'].rolling(i).mean()
            d = dfs[:i].to_list()[-1]
            if str(d) == 'nan':
                data['ma' + str(i)].append(0)
            else:
                data['ma' + str(i)].append(d)

    df = pd.DataFrame(data)
    return df


def main(date, s_table):
    sql = f"select * from {s_table} where create_date = '{date}' and ogc  <1  ORDER by return desc"
    df = pd.read_sql_query(sql, con=engine)
    if len(df) == 0:
        return
    dfs = df['code'].to_list()[:30]
    df_30 = get_stocks(dfs)
    new_df_30 = pd.merge(df, df_30, how='inner', left_on=['code'], right_on=['code'])
    columns = ['code', 'name', 'return', 'open', 'ogc', 'c_0930']
    df_a = new_df_30.loc[
        (new_df_30['ma10'] > new_df_30['ma20']) &
        (new_df_30['c_0930'] < 3)
        , columns].sort_values(by=['return'], ascending=False)
    print(df_a)


"""
         code  name  return   open   ogc  c_0930
0   000802.SZ  北京文化   10.06   5.41 -1.10    2.03
3   002054.SZ  德美化工   10.03  10.97 -1.97    2.92
4   000712.SZ  锦龙股份   10.03  17.75 -0.11   -3.72
7   000048.SZ  京基智农   10.00  26.02 -1.44   -0.08
11  000807.SZ  云铝股份    9.95   8.96  0.11    2.79
13  000425.SZ  徐工机械    8.59   6.40 -0.78   -0.16
14  002747.SZ   埃斯顿    8.45  37.57  0.99   -2.42
15  000680.SZ  山推股份    8.33   3.85 -1.28    0.52
16  002271.SZ  东方雨虹    7.19  55.00 -0.87    0.65
18  002652.SZ  扬子新材    7.05   3.31 -0.90    1.21
19  002707.SZ  众信旅游    7.00   5.39 -2.00    2.41
20  002123.SZ  梦网科技    6.98  14.87  0.00   -0.40
26  000557.SZ  西部创业    6.51   4.04 -1.22    0.50
"""

if __name__ == '__main__':
    db = 'bbb'
    date_format = '%Y-%m-%d'
    d_format = '%Y%m%d'
    t_format = '%H%M'
    # 获得当天
    dd = datetime.now()
    start_date = dd - timedelta(days=31)
    end_date = dd - timedelta(days=1)
    cur_t = dd.strftime(t_format)
    # ts初始化
    ts_data = ts.pro_api('d256364e28603e69dc6362aefb8eab76613b704035ee97b555ac79ab')
    df = ts_data.trade_cal(exchange='', start_date=start_date.strftime(d_format),
                           end_date=end_date.strftime(d_format), is_open='1')
    last_d = df.tail(1)['cal_date'].to_list()[0]
    cur_date = datetime.strptime(last_d, d_format)
    last_date = cur_date.strftime(date_format)
    print(last_date)
    # 创建连接引擎
    engine = create_engine(f'sqlite:///{db}/{db}.db', echo=False, encoding='utf-8')
    conn = engine.connect()
    trans = conn.begin()
    s_table = 'zjlx'
    main(last_date, s_table)
