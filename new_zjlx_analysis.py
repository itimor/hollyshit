# -*- coding: utf-8 -*-
# author: itimor
# 统计资金流向，找出涨幅的资金分布区

from datetime import datetime, timedelta
from sqlalchemy import create_engine
import pandas as pd
import numpy as np

df_rank = {'v1': 'return >= 9',
           'v2': 'return >= 8 and return < 9',
           'v3': 'return >= 7 and return < 8',
           'v4': 'return >= 6 and return < 7',
           'v5': 'return >= 7 and return < 6',
           'v6': 'return >= 3 and return < 5',
           'v7': 'return >= 0 and return < 3',
           }

area_level = [i for i in range(31)]
label_level = [i for i in range(30)]
tactics = ['master', 'super', 'big', 'mid', 'small']


def get_stocks(table):
    df = pd.DataFrame({
        'l1': pd.Series(label_level),
    })
    for tactic in tactics:
        tactic = f'{tactic}_x'
        for rank_level, v in df_rank.items():
            try:
                df_a = pd.read_sql_query(f'select {tactic} from {table} where {v}', con=engine)
            except:
                continue
            print(f'{table} {tactic} {rank_level}')
            if len(df_a) > 0:
                a = df_a[tactic].to_list()
            else:
                a = [0] * len(label_level)
            cut = pd.cut(a, area_level, labels=label_level)
            b = cut.value_counts().sort_index().to_list()
            df[rank_level] = b
            df[rank_level] = df[rank_level] / df[rank_level].sum()
            df = df.replace(np.nan, 0)
            df.to_sql(f'{table}_{tactic}', con=engine, index=False, if_exists='replace')


if __name__ == '__main__':
    db = 'aaa'
    date_format = '%Y-%m-%d'
    d_format = '%Y%m%d'
    t_format = '%H%M'
    # 获得当天
    dd = datetime.now()
    cur_date = dd.strftime(date_format)
    if dd.hour > 19:
        tables = [datetime.strftime(x, t_format) for x in
                  pd.date_range(f'{cur_date} 09:50', f'{cur_date} 11:20:00', freq='10min')]
        # 创建连接引擎
        engine = create_engine(f'sqlite:///{cur_date}/{db}.db', echo=False, encoding='utf-8')
        table = 'aaa_1000_1010'
        get_stocks(table)

