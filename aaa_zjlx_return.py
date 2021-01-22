# -*- coding: utf-8 -*-
# author: itimor
# 东方财富资金流向，并根据策略筛选股票

from datetime import datetime, timedelta
from sqlalchemy import create_engine
import pandas as pd
import numpy as np


def gen_analysis(table):
    df_rank = {'v1': 'return >= 9',
               'v2': 'return >= 8 and return < 9',
               'v3': 'return >= 7 and return < 8',
               'v4': 'return >= 6 and return < 7',
               'v5': 'return >= 7 and return < 6',
               'v6': 'return >= 3 and return < 5',
               'v7': 'return >= 0 and return < 3',
               }

    area_level = [i for i in range(11)]
    label_level = [i for i in range(10)]
    tactics = ['master', 'super', 'big', 'mid', 'small']

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


def get_stocks():
    df_1600 = pd.read_sql_query(f'select code,name,close,return from {db}_1600', con=engine)
    last_df = pd.DataFrame()
    last_time = ''
    for zjlx in t_list:
        table = f'{db}_{zjlx}'
        print(table)
        try:
            df = pd.read_sql_query(f'select * from {table}', con=engine)
            if len(df) == 0:
                continue
        except Exception as e:
            continue
        if len(last_df) == 0:
            last_df = df
            last_time = zjlx
            continue
        else:
            df_a = pd.merge(last_df, df, on=['code', 'name'], suffixes=['_0', '_1'])
            columns = ['return', 'master', 'super', 'big', 'mid', 'small']
            for column in columns:
                df_a[column + '_x'] = df_a[column + '_1'] - df_a[column + '_0']
            result = pd.merge(df_a, df_1600, how='left', on=['code', 'name'])
            columns = ['code', 'name', 'close', 'return',
                       'return_x', 'master_x', 'super_x', 'big_x', 'mid_x', 'small_x']
            new_df = result.loc[(result["return_x"] > 0), columns]
            print(new_df[:5])
            new_table = f'{db}_{last_time}_{zjlx}'
            new_df.to_sql(new_table, con=engine, index=False, if_exists='replace')
            gen_analysis(new_table)
            last_df = df
            last_time = zjlx
        df_b = pd.merge(df, df_1600, on=['code', 'name'], suffixes=['_0', '_1'])
        df_b.to_sql(table, con=engine, index=False, if_exists='replace')


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
    engine = create_engine(f'sqlite:///{cur_date}/{db}.db', echo=False, encoding='utf-8')
    get_stocks()

