# -*- coding: utf-8 -*-
# author: itimor

import requests
from fake_useragent import UserAgent
from collections import Counter

ua = UserAgent()

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


def get_ssq_lottery():
    headers = {
        'referer': 'http://www.cwl.gov.cn/kjxx/wqkj/',
        'User-Agent': ua.random
    }
    params = {
        'name': 'ssq',
        'issueCount': 100
    }
    r = requests.get('http://www.cwl.gov.cn/cwl_admin/kjxx/findDrawNotice', params=params, headers=headers)
    if r.status_code == 200:
        return r.json()['result']
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
        i += n
        all_ball.append(blue_list[i])
    b = Counter(all_ball)
    result = [i for i in b.keys()]
    return result


if __name__ == '__main__':
    qiu_list = get_ssq_lottery()
    red_list = [i['red'].split(',') for i in qiu_list]
    blue_list = [i['blue'] for i in qiu_list]
    # print(red_list)
    # print(blue_list)
    # red_list = [['03', '08', '09', '13', '15', '18'], ['06', '14', '16', '26', '28', '29'], ['04', '15', '21', '25', '29', '33'], ['06', '09', '12', '16', '27', '31'], ['03', '06', '14', '18', '20', '26'], ['05', '10', '16', '23', '27', '33'], ['01', '04', '11', '19', '32', '33'], ['02', '04', '07', '24', '25', '32'], ['01', '05', '07', '14', '18', '33'], ['02', '04', '12', '21', '25', '32'], ['06', '08', '22', '24', '25', '26'], ['07', '09', '14', '26', '30', '31'], ['02', '03', '07', '08', '17', '22'], ['06', '09', '16', '18', '22', '29'], ['06', '10', '13', '25', '26', '32'], ['02', '03', '13', '18', '20', '31'], ['02', '09', '10', '20', '22', '26'], ['03', '19', '22', '23', '27', '29'], ['01', '04', '11', '12', '14', '23'], ['08', '19', '22', '26', '27', '30'], ['08', '09', '11', '14', '17', '29'], ['01', '02', '05', '15', '28', '33'], ['08', '10', '15', '16', '23', '26'], ['01', '04', '18', '19', '26', '31'], ['03', '07', '12', '14', '23', '28'], ['05', '12', '16', '26', '30', '31'], ['16', '18', '19', '20', '29', '33'], ['01', '03', '18', '19', '26', '29'], ['12', '15', '17', '24', '26', '31'], ['02', '04', '06', '21', '25', '29'], ['01', '02', '03', '04', '09', '10'], ['01', '09', '22', '28', '32', '33'], ['11', '13', '19', '26', '30', '33'], ['07', '08', '09', '10', '16', '27'], ['05', '06', '14', '16', '19', '27'], ['01', '04', '12', '20', '25', '32'], ['01', '05', '11', '24', '30', '32'], ['01', '03', '07', '10', '22', '32'], ['02', '09', '12', '17', '28', '32'], ['06', '09', '17', '22', '24', '26'], ['02', '06', '09', '14', '22', '25'], ['06', '13', '16', '20', '23', '32'], ['10', '12', '15', '17', '23', '32'], ['03', '09', '11', '24', '25', '28'], ['02', '04', '11', '15', '18', '28'], ['06', '14', '19', '20', '22', '24'], ['04', '08', '10', '16', '27', '29'], ['04', '09', '10', '22', '28', '32'], ['09', '10', '19', '25', '26', '29'], ['10', '17', '24', '25', '28', '30'], ['01', '07', '15', '16', '20', '23'], ['05', '06', '11', '12', '15', '30'], ['06', '08', '11', '22', '25', '33'], ['02', '08', '21', '25', '26', '30'], ['01', '20', '23', '26', '27', '32'], ['03', '09', '16', '17', '20', '26'], ['10', '15', '17', '27', '29', '31'], ['04', '05', '10', '13', '15', '19'], ['01', '06', '12', '18', '22', '24'], ['01', '09', '11', '12', '16', '19'], ['02', '21', '23', '26', '31', '32'], ['02', '14', '16', '21', '29', '30'], ['01', '06', '12', '26', '29', '30'], ['11', '15', '20', '23', '25', '33'], ['02', '04', '06', '15', '24', '27'], ['01', '02', '05', '09', '19', '24'], ['03', '07', '16', '17', '23', '30'], ['01', '19', '25', '26', '30', '31'], ['02', '08', '11', '17', '21', '30'], ['01', '05', '13', '14', '27', '33'], ['14', '15', '18', '22', '31', '33'], ['05', '12', '20', '21', '22', '29'], ['03', '11', '14', '16', '21', '32'], ['03', '10', '16', '21', '25', '27'], ['10', '15', '16', '18', '20', '27'], ['03', '11', '13', '20', '24', '30'], ['04', '08', '09', '13', '19', '33'], ['05', '07', '11', '13', '27', '29'], ['06', '08', '10', '15', '17', '26'], ['09', '11', '12', '13', '22', '23'], ['01', '02', '04', '06', '19', '21'], ['03', '09', '10', '13', '18', '26'], ['12', '16', '21', '26', '27', '32'], ['04', '07', '09', '23', '27', '30'], ['02', '09', '13', '17', '26', '28'], ['09', '15', '18', '21', '23', '26'], ['01', '03', '07', '21', '27', '32'], ['12', '15', '16', '22', '29', '32'], ['10', '14', '17', '22', '26', '27'], ['08', '17', '24', '26', '27', '31'], ['05', '09', '14', '20', '24', '30'], ['02', '04', '10', '17', '22', '25'], ['01', '03', '11', '12', '19', '26'], ['09', '14', '21', '23', '26', '32'], ['02', '05', '08', '12', '26', '31'], ['01', '05', '07', '23', '28', '30'], ['03', '10', '19', '25', '26', '31'], ['02', '14', '15', '16', '32', '33'], ['02', '08', '13', '29', '32', '33'], ['03', '06', '08', '11', '19', '28']]
    # blue_list = ['10', '07', '06', '06', '01', '14', '05', '13', '07', '16', '01', '04', '15', '11', '11', '11', '01', '07', '04', '07', '16', '04', '10', '07', '11', '13', '12', '03', '15', '03', '12', '15', '05', '07', '10', '02', '03', '11', '05', '16', '04', '13', '05', '16', '10', '01', '09', '08', '08', '15', '07', '12', '02', '10', '15', '02', '08', '15', '03', '16', '06', '10', '12', '10', '06', '16', '07', '12', '09', '15', '01', '14', '04', '12', '06', '16', '12', '03', '04', '08', '15', '04', '10', '08', '07', '08', '01', '14', '05', '04', '08', '14', '07', '03', '14', '12', '02', '01', '15', '08']
    # 选几组号码
    n = 4
    print("# 福彩 双色球")
    for k, v in enumerate(pai[:n]):
        a = get_red_result(v)[:6]
        b = get_blue_result(v)[:6]
        red = ' '.join(a)
        blue = ' '.join(b)
        r = f'红球：{red} 蓝球：{b[v]}'
        print(r)
    """
    #双色球
    红球：04 15 21 25 29 33 蓝球：01
    红球：04 15 21 25 29 33 蓝球：05
    红球：06 09 12 16 27 31 蓝球：04
    """
