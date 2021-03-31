# -*- coding: utf-8 -*-
# author: itimor
# 东方财富资金流向，并根据策略筛选股票

from datetime import datetime, timedelta
from fake_useragent import UserAgent
from sqlalchemy import create_engine
import pandas as pd
import tushare as ts

ua = UserAgent()

# 今日尾盘入  明日大涨 后天大大涨
"""
SELECT * from b_new where ogc <-2 and master > 10 and change < 6 ORDER by master;
"""
# 今日早盘入  明日大涨 后天大大涨
"""
SELECT * from b_new where ogc <0 and master < 20 and big > 0 and small < 0 and close < 10 ORDER by ogc;
"""


def get_stocks(codes):
    code_list = ','.join(codes)
    df = ts_data.daily(ts_code=code_list, start_date=start_date, end_date=end_date)
    df_a = df.rename(
        columns={'ts_code': 'code', 'close': close_x}
    )
    return df_a


def main(date, s_table, t):
    sql = f"select * from {s_table} where create_date = '{date}'"
    df = pd.read_sql_query(sql, con=engine)
    dfs = get_stocks(df['code'].to_list())

    if len(dfs) == 0:
        return
    new_df = pd.merge(df, dfs, how='inner', left_on=['code'], right_on=['code'])
    new_df[return_x] = (new_df[close_x] - new_df['open']) / new_df['open'] * 100
    df.drop([close_x], axis=1, inplace=True)
    if dd.hour > 15:
        df_a = new_df.sort_values(by=['ogc'], ascending=True)
        print(df_a.head())
        try:
            # delete those rows that we are going to "upsert"
            conn.execute(f"delete from {s_table} where create_date = '{date}'")
            trans.commit()

            # insert changed rows
            df_a.to_sql(s_table, engine, if_exists='append', index=True)
        except:
            trans.rollback()
            raise


if __name__ == '__main__':
    db = 'bbb'
    date_format = '%Y-%m-%d'
    d_format = '%Y%m%d'
    t_format = '%H%M'
    # 获得当天
    dd = datetime.now()
    cur_t = dd.strftime(t_format)
    if dd.hour > 8:
        # ts初始化
        ts_data = ts.pro_api('d256364e28603e69dc6362aefb8eab76613b704035ee97b555ac79ab')
        last_d = "2021-01-28"
        t = '1'  # 1 3 5 10
        close_x = f'close_{t}'
        return_x = f'return_{t}'
        dd = datetime.strptime(last_d, d_format)
        start_date = dd + timedelta(days=t)
        end_date = dd + timedelta(days=t)
        # 创建连接引擎
        engine = create_engine(f'sqlite:///{db}/{db}.db', echo=False, encoding='utf-8')
        conn = engine.connect()
        trans = conn.begin()
        s_table = f'jgdy'
        main(last_d, s_table)
