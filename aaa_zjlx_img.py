# -*- coding: utf-8 -*-
# author: itimor
# 统计资金流向，找出涨幅的资金分布区

from datetime import datetime, timedelta
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import tushare as ts
import matplotlib.pyplot as plt

colunms_name = ['date', 'l1', 'v1', 'v2', 'v3', 'v4', 'v5', 'v6', 'v7']
tactics = ['master', 'super', 'big']


# 1100
# super 最大前三 涨幅>0  < 2
# mid 最小前五 涨幅>0
# small 最大前五 涨幅>0


def get_stocks():
    for zjlx in tables:
        for tactic in tactics:
            table = f'{db}_{zjlx}_{tactic}'
            print(table)
            try:
                df_zjlx_ranks_1_master = pd.read_sql_query(f'select * from {table}', con=engine)
            except:
                continue
            df_zjlx_ranks_1_master.columns = colunms_name
            df_zjlx_ranks_1_master.plot(x='l1')
            # 图片标题
            plt.title(table)
            # x坐标轴文本
            plt.xlabel('资金流入占比')
            # y坐标轴文本
            plt.ylabel('涨幅占比')
            # 显示网格
            plt.grid(True)


if __name__ == '__main__':
    db = 'aaa'
    date_format = '%Y-%m-%d'
    d_format = '%Y%m%d'
    t_format = '%H%M'
    # 获得当天
    dd = datetime.now()
    cur_date = dd.strftime(date_format)
    if dd.hour > 19:
        t_list_am = [datetime.strftime(x, t_format) for x in
                     pd.date_range(f'{cur_date} 09:30', f'{cur_date} 11:30:00', freq='10min')]
        t_list_pm = [datetime.strftime(x, t_format) for x in
                     pd.date_range(f'{cur_date} 13:10', f'{cur_date} 14:50:00', freq='10min')]
        t_list = t_list_am + t_list_pm
        tables = []
        for i, v in enumerate(t_list):
            if i % 2 == 0:
                b = v
            else:
                tables.append(f'{b}_{v}')
                b = ''
        # 创建连接引擎
        engine = create_engine(f'sqlite:///{cur_date}/{db}.db', echo=False, encoding='utf-8')
        get_stocks()
        # 显示图形
        plt.show()

