# -*- coding: utf-8 -*-
# author: itimor

import requests
from fake_useragent import UserAgent
from collections import Counter

ua = UserAgent()


def fib_loop_while(n):
    a, b = 1, 1
    for i in range(0, n):
        a, b = b, a + b
        yield a


pai = [i for i in fib_loop_while(10)]


def get_dlt_lottery():
    headers = {
        'referer': 'https://www.lottery.gov.cn/kj/kjlb.html?dlt',
        'User-Agent': ua.random
    }
    params = {
        'gameNo': 85,
        'provinceId': 0,
        'pageSize': 100,
        'isVerify': 1,
        'pageNo': 1,
    }
    r = requests.get('https://webapi.sporttery.cn/gateway/lottery/getHistoryPageListV1.qry', params=params,
                     headers=headers)
    if r.status_code == 200:
        return r.json()['value']['list']
    else:
        return r.text


def get_red_result(n):
    all_ball = []
    for i in pai:
        i += n
        all_ball += red_list[i]

    b = Counter(all_ball)
    result = [i for i in b.keys()]
    return result


def get_blue_result(n):
    all_ball = []
    for i in pai:
        i += n * 2
        all_ball += blue_list[i]

    b = Counter(all_ball)
    result = [i for i in b.keys()]
    return result


if __name__ == '__main__':
    qiu_list = get_dlt_lottery()
    red_list = [i['lotteryDrawResult'].split()[:5] for i in qiu_list]
    blue_list = [i['lotteryDrawResult'].split()[5:] for i in qiu_list]
    # print(red_list)
    # print(blue_list)
    # red_list = [['17', '21', '26', '29', '32'], ['08', '09', '10', '19', '34'], ['04', '08', '17', '19', '25'], ['07', '13', '20', '27', '29'], ['21', '27', '28', '30', '34'], ['06', '12', '20', '23', '33'], ['06', '09', '11', '14', '21'], ['01', '05', '16', '22', '34'], ['01', '03', '20', '29', '32'], ['05', '19', '20', '27', '33'], ['05', '23', '30', '34', '35'], ['04', '22', '25', '28', '35'], ['03', '07', '10', '12', '35'], ['05', '10', '17', '23', '28'], ['07', '21', '28', '31', '35'], ['02', '16', '26', '31', '34'], ['02', '06', '12', '19', '33'], ['08', '11', '13', '22', '25'], ['11', '24', '26', '33', '35'], ['03', '15', '16', '17', '21'], ['08', '25', '27', '29', '35'], ['16', '17', '23', '26', '32'], ['10', '22', '27', '33', '34'], ['09', '12', '13', '26', '33'], ['01', '04', '17', '18', '26'], ['01', '04', '09', '22', '28'], ['01', '02', '03', '08', '22'], ['11', '15', '24', '26', '35'], ['01', '07', '11', '15', '21'], ['08', '19', '29', '34', '35'], ['09', '17', '27', '28', '35'], ['05', '25', '26', '30', '35'], ['14', '22', '24', '28', '32'], ['01', '09', '15', '30', '33'], ['07', '19', '20', '23', '24'], ['08', '10', '12', '16', '22'], ['01', '09', '18', '19', '23'], ['10', '12', '21', '24', '30'], ['06', '14', '18', '28', '33'], ['08', '11', '13', '30', '32'], ['05', '07', '09', '20', '28'], ['11', '16', '20', '21', '28'], ['02', '20', '21', '23', '32'], ['02', '03', '06', '07', '33'], ['01', '04', '13', '17', '28'], ['02', '03', '11', '14', '27'], ['03', '08', '12', '13', '22'], ['03', '12', '23', '26', '30'], ['07', '12', '13', '19', '23'], ['07', '11', '18', '20', '29'], ['03', '04', '05', '09', '34'], ['04', '05', '08', '22', '33'], ['02', '03', '11', '15', '27'], ['04', '09', '15', '24', '35'], ['05', '25', '27', '30', '34'], ['07', '08', '10', '25', '28'], ['02', '18', '25', '26', '32'], ['04', '06', '07', '08', '16'], ['06', '14', '25', '27', '28'], ['08', '11', '17', '31', '35'], ['12', '16', '20', '23', '31'], ['05', '06', '27', '28', '29'], ['13', '16', '20', '22', '27'], ['02', '07', '23', '26', '35'], ['01', '04', '20', '23', '29'], ['08', '11', '15', '20', '28'], ['03', '04', '11', '15', '28'], ['02', '10', '20', '24', '31'], ['01', '02', '17', '32', '34'], ['02', '11', '12', '21', '34'], ['01', '13', '15', '32', '35'], ['03', '06', '22', '24', '25'], ['04', '16', '24', '29', '35'], ['14', '18', '20', '28', '34'], ['17', '23', '28', '29', '32'], ['01', '08', '18', '28', '30'], ['02', '09', '10', '21', '35'], ['03', '09', '10', '12', '21'], ['04', '10', '12', '23', '27'], ['11', '15', '18', '20', '27'], ['01', '12', '16', '28', '33'], ['01', '22', '30', '31', '32'], ['05', '07', '26', '30', '31'], ['15', '20', '23', '26', '33'], ['01', '08', '14', '16', '28'], ['06', '14', '21', '34', '35'], ['13', '15', '26', '32', '33'], ['05', '10', '23', '34', '35'], ['11', '18', '20', '21', '33'], ['01', '05', '14', '23', '31'], ['07', '08', '12', '21', '26'], ['12', '14', '25', '34', '35'], ['01', '15', '22', '28', '32'], ['01', '15', '19', '26', '27'], ['17', '20', '21', '22', '31'], ['03', '11', '25', '29', '34'], ['03', '04', '12', '18', '34'], ['11', '14', '15', '19', '20'], ['02', '15', '18', '21', '27'], ['09', '11', '26', '28', '35']]
    # blue_list = [['02', '07'], ['01', '02'], ['01', '07'], ['04', '09'], ['08', '11'], ['01', '09'], ['01', '03'], ['08', '11'], ['05', '12'], ['03', '08'], ['08', '10'], ['04', '05'], ['03', '05'], ['02', '08'], ['04', '10'], ['09', '11'], ['08', '09'], ['04', '07'], ['08', '09'], ['07', '10'], ['02', '03'], ['02', '12'], ['01', '11'], ['04', '10'], ['03', '10'], ['04', '12'], ['01', '04'], ['04', '05'], ['04', '11'], ['06', '11'], ['03', '08'], ['04', '10'], ['07', '08'], ['04', '09'], ['05', '10'], ['03', '09'], ['01', '09'], ['03', '07'], ['02', '05'], ['02', '06'], ['02', '07'], ['05', '11'], ['06', '12'], ['08', '09'], ['03', '07'], ['04', '10'], ['04', '12'], ['04', '07'], ['02', '08'], ['09', '12'], ['07', '09'], ['07', '10'], ['04', '12'], ['04', '10'], ['06', '12'], ['03', '11'], ['02', '07'], ['02', '08'], ['04', '11'], ['07', '11'], ['05', '12'], ['11', '12'], ['01', '02'], ['01', '12'], ['04', '08'], ['04', '10'], ['02', '03'], ['06', '12'], ['04', '09'], ['04', '08'], ['08', '10'], ['09', '10'], ['02', '12'], ['10', '11'], ['01', '03'], ['10', '12'], ['01', '07'], ['04', '11'], ['08', '09'], ['05', '06'], ['03', '12'], ['04', '12'], ['07', '10'], ['02', '08'], ['04', '10'], ['03', '10'], ['05', '07'], ['01', '11'], ['06', '07'], ['02', '07'], ['01', '06'], ['03', '12'], ['03', '11'], ['05', '10'], ['02', '08'], ['03', '06'], ['10', '12'], ['03', '07'], ['03', '06'], ['06', '07']]
    # 选几组号码
    n = 4
    print("# 体彩 大乐透")
    for k, v in enumerate(pai[:n]):
        a = get_red_result(v)[:5]
        b = get_blue_result(v)[:2]
        red = ' '.join(a)
        blue = ' '.join(b)
        r = f'红球：{red} 蓝球：{blue}'
        print(r)
    """
    # 大乐透
    红球：04 08 17 19 25 蓝球：04 09
    红球：07 13 20 27 29 蓝球：01 09
    红球：21 27 28 30 34 蓝球：08 11
    红球：06 09 11 14 21 蓝球：04 05
    """