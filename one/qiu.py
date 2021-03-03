# -*- coding: utf-8 -*-
# author: timor

from random import shuffle, sample
from time import time


def display(type, balls):
    """
    输出列表中的双色球号码
    """
    if type == 'ssq':
        n = 1
    else:
        n = 2

    for index, ball in enumerate(balls):
        if index == len(balls) - n:
            print('|', end=' ')
        print('%02d' % ball, end=' ')
    print()


def gun(lst):
    a = str(time())
    b = a[::-1]
    c = str(int(float(b)))
    s = 0
    for i in c:
        s += int(i)
    for _ in range(s):
        shuffle(lst)
    return lst


def random_select(info, red_num, blue_num):
    """
    随机选择一组号码
    """

    red_nums = list(range(1, info['redmax']))
    [red_nums.remove(i) for i in red_num]
    red_balls = []
    for _ in range(info['redchoose']):
        red_nums = gun(red_nums)
        qiu = red_nums.pop()
        red_balls.append(qiu)
    red_balls.sort()

    blue_nums = list(range(1, info['bluemax']))
    [blue_nums.remove(i) for i in blue_num]
    blue_balls = []
    for _ in range(info['bluechoose']):
        blue_nums = gun(blue_nums)
        qiu = blue_nums.pop()
        blue_balls.append(qiu)
    blue_balls.sort()
    display(type, red_balls + blue_balls)
    return red_balls + blue_balls


def gen_double(type, m, red_num, blue_num):
    lst = []
    if type == 'ssq':
        info = {
            'redmax': 33 + 1,
            'bluemax': 16 + 1,
            'redchoose': 6,
            'bluechoose': 1,
        }
    else:
        info = {
            'redmax': 35 + 1,
            'bluemax': 12 + 1,
            'redchoose': 5,
            'bluechoose': 2,
        }
    for _ in range(m):
        lst.append(random_select(info, red_num, blue_num))
    return {'all_ball': lst, 'god_ball': sample(lst, 2)}


if __name__ == '__main__':
    type = 'ssq'
    # type = 'dlt'
    remove_red = [3, 5]
    remove_blue = [3, 5]
    m = 20
    print(gen_double(type, m, remove_red, remove_blue))