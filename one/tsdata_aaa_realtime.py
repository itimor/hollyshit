# -*- coding: utf-8 -*-
# author: itimor
# 东方财富资金流向，并根据策略筛选股票

from datetime import datetime, timedelta
from sqlalchemy import create_engine
import pandas as pd
import tushare as ts
from one.send_msg import to_ding


# 今日尾盘入  明日大涨 后天大大涨
"""
SELECT * from b_new where ogc <-2 and master > 10 and change < 6 ORDER by master;
"""
# 今日早盘入  明日大涨 后天大大涨
"""
SELECT * from b_new where ogc <0 and master < 20 and big > 0 and small < 0 and close < 10 ORDER by ogc;
SELECT * from b_new_0930 where abs(ogc) <1 AND master < 10 AND master > 4 AND super < 12 AND super > 0 AND big < 0 AND small < 0 AND mid < 0 AND change < 5 ORDER by master;
"""


# 获取实时行情-tushare接口
def get_stocks_tushare():
    df = ts.get_today_all()
    columns = ['code', 'now', 'open_x', 'ogc']
    df['ogc'] = (df['open'] - df['settlement']) / df['settlement'] * 100
    dfs = df.rename(columns={'ts_code': columns[0], 'trade': columns[1], 'open': columns[2]})
    s_codes = []
    for i in dfs['code']:
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
    dfs['code'] = s_codes
    last_df = dfs[columns]
    return last_df


def main(date, s_table, cur_t):
    print(date)
    sql = f"select * from {s_table} where trade_date = '{date}'"
    df = pd.read_sql_query(sql, con=engine)
    df.drop(['ogc'], axis=1, inplace=True)
    print(df.head())
    if len(df) == 0:
        return
    dfs = get_stocks_tushare()
    print(dfs.head())
    if len(dfs) > 0:
        change = f'c_{cur_t}'
        new_df = pd.merge(df, dfs, how='inner', left_on=['code'], right_on=['code'])
        if len(new_df) == 0:
            return
        print(new_df.head())
        try:
            new_df[change] = (new_df['now'] - new_df['open_x']) / new_df['open_x'] * 100
        except:
            new_df[change] = 0
        df_a = new_df.sort_values(by=['ogc'], ascending=True).set_index('trade_date').round({change: 2, 'ogc': 2})
        df_a.drop(['now', 'open_x'], axis=1, inplace=True)
        print(df_a.head())

        try:
            conn.execute(f"delete from {s_table} where trade_date = '{date}' and code in {tuple(df_a['code'].to_list())}")
            trans.commit()
            df_a.to_sql(s_table, engine, if_exists='append', index=True)
        except:
            trans.rollback()
            raise

        if cur_t == '0930':
            columns = ['code', 'name', 'return', 'open_x', 'ogc', change]
            df_b = new_df.loc[
                (new_df[change] > 0.95) &
                (new_df[change] < 1.02) &
                (new_df['ogc'] < 1) &
                (new_df[change] < 3)
                , columns].sort_values(by=['return'], ascending=False)
            if len(df_b) > 0:
                last_df = df_b[:5].to_string(header=None)
                print(last_df)
                last_df.to_sql('tsdata_aaa', engine, if_exists='replace', index=False)
                text = '%s 涨幅小于1大于0.95，高开小于1\n' % date + last_df
                to_ding(text)


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
    c_time = 9
    if dd.hour == c_time:
        # ts初始化
        ts_data = ts.pro_api('d256364e28603e69dc6362aefb8eab76613b704035ee97b555ac79ab')
        df_ts = ts_data.trade_cal(exchange='', start_date=start_date.strftime(d_format),
                                  end_date=end_date.strftime(d_format), is_open='1')
        last_d = df_ts.tail(1)['cal_date'].to_list()[0]
        cur_date = datetime.strptime(last_d, d_format)
        last_date = cur_date.strftime(date_format)
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
        if dd.hour == c_time:
            cur_t = '0930'
        if cur_t in t_list:
            main(last_d, s_table, cur_t)
