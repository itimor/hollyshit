import re

with open('1', 'r', encoding='UTF-8') as fn:
    lines = fn.readlines()
    for line in lines:
        b = line.split()
        z = float(b[0])
        y = float(b[1])
        x = float(b[2])
        s = float(10 / x * 7) + float(10 / y * 5) + float(10 / 6 * 2)
        print(s)
