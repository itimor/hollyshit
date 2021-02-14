# -*- coding: utf-8 -*-
# author: itimor
# 东方财富资金流向，并根据策略筛选股票

from datetime import datetime, timedelta
from sqlalchemy import create_engine
from telegram import Bot, ParseMode
import pandas as pd
import tushare as ts


day_list = [3, 5, 10, 20]

def get_stocks(codes):
    data = {
        'code': codes,
        'ma5': [],
        'ma10': [],
        'ma20': [],
    }
    for code in codes:
        df_code = ts_data.daily(ts_code=code, start_date=start_date.strftime(d_format),
                                end_date=end_date.strftime(d_format))
	for i in day_list:
            dfs = df_code['close'].rolling(i).mean()
            d = dfs[:i].to_list()[-1]
            if str(d) == 'nan':
                data['ma' + str(i)].append(0)
            else:
                data['ma' + str(i)].append(d)

    df = pd.DataFrame(data)
    return df


def sort_stocks(df_boy):
    codes = df_boy['code'].to_list()
    df_a = df_boy.sort_values(by=['ma10_20'], ascending=False)
    df_a_1 = df_a.reset_index(drop=True)
    df_a_1.index += 1
    df_b = df_boy.sort_values(by=['c_0930'], ascending=False)
    df_b_1 = df_b.reset_index(drop=True)
    df_b_1.index += 1
    df_c = df_boy.sort_values(by=['ogc'], ascending=True)
    df_c_1 = df_c.reset_index(drop=True)
    df_c_1.index += 1
    df_d = df_boy.sort_values(by=['return'], ascending=False)
    df_d_1 = df_d.reset_index(drop=True)
    df_d_1.index += 1

    data = {
        'code': codes,
        'ma_x': [],
        'c_9030_x': [],
        'ogc_x': [],
        'return_x': [],
    }
    for code in codes:
        print(code)
        a = df_a_1.index[df_a_1['code'] == code].to_list()[0]
        data['ma_x'].append(a)
        b = df_b_1.index[df_b_1['code'] == code].to_list()[0]
        data['c_9030_x'].append(b)
        c = df_c_1.index[df_c_1['code'] == code].to_list()[0]
        data['ogc_x'].append(c)
        d = df_d_1.index[df_d_1['code'] == code].to_list()[0]
        data['return_x'].append(d)

    df = pd.DataFrame(data)
    return df


def send_tg(text, chat_id):
    token = '723532221:AAH8SSfM7SfTe4HmhV72QdLbOUW3akphUL8'
    bot = Bot(token=token)
    chat_id = chat_id
    bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.HTML)


def main(date, s_table):
    sql = f"select * from {s_table} where create_date = '{date}' and ogc  <1.02  ORDER by return desc"
    df = pd.read_sql_query(sql, con=engine)
    if len(df) == 0:
        return
    codes = df['code'].to_list()[:30]
    df_30 = get_stocks(codes)
    new_df_30 = pd.merge(df, df_30, how='inner', left_on=['code'], right_on=['code'])
    new_df_30['ma3_5'] = (new_df_30['ma3'] - new_df_30['ma5']) / new_df_30['ma5']
    new_df_30['ma5_10'] = (new_df_30['ma5'] - new_df_30['ma10']) / new_df_30['ma10']
    new_df_30['ma10_20'] = (new_df_30['ma10'] - new_df_30['ma20']) / new_df_30['ma20']
    columns = ['code', 'name', 'return', 'open', 'ogc', 'c_0930', 'ma10_20']
    df_boy = new_df_30.loc[
        (new_df_30['ma10'] > new_df_30['ma20']) &
        (new_df_30['ma10_20'] > 0.01) &
        (new_df_30['c_0930'] < 3) &
        (new_df_30['c_0930'] > -1) &
        (new_df_30['close'] < 50)
        , columns].sort_values(by=['return'], ascending=False)
    df_sort = sort_stocks(df_boy)
    new_df_sort = pd.merge(df_sort, df, how='inner', left_on=['code'], right_on=['code'])
    weight = [5, 3, 2, 1]
    new_df_sort['s'] = 10 / new_df_sort['ma_x'] * weight[0] + 10 / new_df_sort['c_9030_x'] * weight[1] + 10 / \
                       new_df_sort['ogc_x'] * weight[2] + 10 / new_df_sort['return_x'] * weight[3]
    columns = ['code', 'name', 'return', 'open', 'ogc', 'c_0930', 's']
    df_b = new_df_sort[columns].sort_values(by=['s'], ascending=False)
    if len(df_b) > 0:
        last_df = df_b[:10].to_string(header=None)
        chat_id = "@hollystock"
        text = '%s 开服小于1.02, ma排序从大到小\n' % date + last_df
        send_tg(text, chat_id)
    last_df.to_sql('ma', con=engine, index=True, if_exists='append')


if __name__ == '__main__':
    db = 'bbb'
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
    ts_data = ts.pro_api('d256364e28603e69dc6362aefb8eab76613b704035ee97b555ac79ab')
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
    s_table = 'zjlx'
    main(last_date, s_table)

