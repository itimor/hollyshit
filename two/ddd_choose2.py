# -*- coding: utf-8 -*-
# author: itimor
# 东方财富资金流向，并根据策略筛选股票

from datetime import datetime, timedelta
from sqlalchemy import create_engine
import pandas as pd
import tushare as ts

# 设置最大列数，避免只显示部分列
pd.set_option('display.max_columns', 1000)
# 设置最大行数，避免只显示部分行数据
pd.set_option('display.max_rows', 1000)
# 设置显示宽度
pd.set_option('display.width', 1000)
# 设置每列最大宽度，避免属性值或列名显示不全
pd.set_option('display.max_colwidth', 1000)


def handle(df):
    # n天涨幅
    df['return_0'] = (df['close'] - df['low']) / df['low']
    df['return_1'] = (df['close'].shift(1) - df['open'].shift(1)) / df['open'].shift(1)
    df['return_2'] = (df['close'].shift(2) - df['open'].shift(2)) / df['open'].shift(2)
    df['return_3'] = (df['close'].shift(3) - df['open'].shift(3)) / df['open'].shift(3)
    df['return_sum_3'] = (df['close'] - df['open'].shift(2)) / df['open'].shift(2) * 100
    return df


def main(start_date, end_date):
    sql = f"select * from {s_table} where date >= '{start_date} 00:00:00.000000' and date <= '{end_date} 00:00:00.000000' order by date asc"
    df = pd.read_sql_query(sql, con=engine)
    managed_df = df.groupby('code').apply(handle).reset_index(drop=True)
    result_buy = managed_df[
        (managed_df['return_0'] < 0.3) &
        (managed_df['return_0'] > 0.1) &
        (managed_df['return_1'] > 0.01) &
        (managed_df['return_2'] > 0) &
        (managed_df['return_3'] > -0.01) &
        (managed_df['turn'] > 15) &
        (managed_df['hist'] > -2) &
        (managed_df['close'] > 5) &
        (managed_df['close'] < 30)
        ]
    result_buy.to_sql('result_buy', engine, if_exists='replace', index=False)


if __name__ == '__main__':
    db = 'ddd'
    date_format = '%Y-%m-%d'
    d_format = '%Y%m%d'
    t_format = '%H%M'
    # 获得当天
    dd = datetime.now()
    start_date = dd - timedelta(days=100)
    end_date = dd - timedelta(days=1)
    cur_date = dd.strftime(date_format)
    cur_d = dd.strftime(d_format)
    cur_t = dd.strftime(t_format)
    print(f'今天日期 {cur_d}')
    # ts初始化
    ts.set_token('d256364e28603e69dc6362aefb8eab76613b704035ee97b555ac79ab')
    ts_data = ts.pro_api()
    df_ts = ts_data.trade_cal(exchange='', start_date=start_date.strftime(d_format),
                              end_date=dd.strftime(d_format), is_open='1')
    trade_days = df_ts.tail(60)['cal_date'].to_list()
    # 创建连接引擎
    engine = create_engine(f'sqlite:///{db}/{db}.db', echo=False, encoding='utf-8')
    conn = engine.connect()
    trans = conn.begin()
    s_table = 'stock_ma'
    s_date = datetime.strptime(trade_days[0], d_format)
    e_date = datetime.strptime(trade_days[-1], d_format)
    main(s_date.strftime(date_format), e_date.strftime(date_format))
