# -*- coding: utf-8 -*-
# author: itimor
# 东方财富资金流向，并根据策略筛选股票

from datetime import datetime, timedelta
from sqlalchemy import create_engine
from telegram import Bot, ParseMode
import pandas as pd
import tushare as ts


def get_stocks(codes, date):
    date = '2021-02-09'
    code_list = []
    columns = ['open', 'close', 'price_change', 'p_change', 'ma5', 'ma10', 'ma20', 'turnover']
    for pre_code in codes:
        code = pre_code.split('.')
        df = ts.get_hist_data(code[0], start=date, end=date)
        print(df[columns])
        bol = df[df['ma10'] > df['ma20']]
        if len(bol) > 0:
            code_list.append(pre_code)
    return code_list


def send_tg(text, chat_id):
    token = '723532221:AAH8SSfM7SfTe4HmhV72QdLbOUW3akphUL8'
    bot = Bot(token=token)
    chat_id = chat_id
    bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.HTML)


def main():
    s_date = '20210209'
    d_date = '20210210'
    sql = f"select * from tsdata where trade_date = {s_date} and code in (select code from tsdata where trade_date = {d_date} and ogc_x  <1.02) ORDER by return desc limit 30;"
    df = pd.read_sql_query(sql, con=engine)
    if len(df) == 0:
        return
    df_30 = get_stocks(df['code'].to_list(), s_date)
    print(df_30)


if __name__ == '__main__':
    db = 'aaa'
    date_format = '%Y-%m-%d'
    d_format = '%Y%m%d'
    t_format = '%H%M'
    # 获得当天
    dt = '2021-02-05'
    dd = datetime.strptime(dt, date_format)
    dd = datetime.now()
    cur_dt = dd.strftime(date_format)
    print(f'今天日期 {cur_dt}')
    start_date = dd - timedelta(days=31)
    end_date = dd - timedelta(days=1)
    cur_t = dd.strftime(t_format)
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
    s_table = 'tsdata'
    main()
