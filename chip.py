# -*- coding: utf-8 -*-
# author: itimor

from datetime import datetime, timedelta
import pandas as pd
import tushare as ts
import copy

"""
https://blog.csdn.net/santirenpc/article/details/91411628
"""


class ChipDistribution():

    def __init__(self):
        self.Chip = {}  # 当前获利盘
        self.ChipList = {}  # 所有的获利盘的

    # def get_data_from_csv(self):
    #     self.data = pd.read_csv('test.csv')

    def get_data_from_tushare(self, stock, start_date, end_date):
        # df = ts_data.daily(ts_code=stock, start_date=start_date.strftime(d_format), end_date=end_date.strftime(d_format))
        df = ts.get_hist_data(code=stock, start=start_date.strftime(date_format), end=end_date.strftime(date_format))
        df['date'] = df.index
        self.data = df
        self.price = df['close'].to_list()[0]

    def calcuJUN(self, dateT, highT, lowT, volT, TurnoverRateT, A, minD):

        x = []
        l = (highT - lowT) / minD
        for i in range(int(l)):
            x.append(round(lowT + i * minD, 2))
        length = len(x)
        eachV = volT / length
        for i in self.Chip:
            self.Chip[i] = self.Chip[i] * (1 - TurnoverRateT * A)
        for i in x:
            if i in self.Chip:
                self.Chip[i] += eachV * (TurnoverRateT * A)
            else:
                self.Chip[i] = eachV * (TurnoverRateT * A)
        import copy
        self.ChipList[dateT] = copy.deepcopy(self.Chip)

    def calcuSin(self, dateT, highT, lowT, avgT, volT, TurnoverRateT, minD, A):
        x = []

        l = (highT - lowT) / minD
        for i in range(int(l)):
            x.append(round(lowT + i * minD, 2))

        length = len(x)

        # 计算仅仅今日的筹码分布
        tmpChip = {}
        eachV = volT / length

        # 极限法分割去逼近
        for i in x:
            x1 = i
            x2 = i + minD
            h = 2 / (highT - lowT)
            s = 0
            if i < avgT:
                y1 = h / (avgT - lowT) * (x1 - lowT)
                y2 = h / (avgT - lowT) * (x2 - lowT)
                s = minD * (y1 + y2) / 2
                s = s * volT
            else:
                y1 = h / (highT - avgT) * (highT - x1)
                y2 = h / (highT - avgT) * (highT - x2)

                s = minD * (y1 + y2) / 2
                s = s * volT
            tmpChip[i] = s

        for i in self.Chip:
            self.Chip[i] = self.Chip[i] * (1 - TurnoverRateT * A)

        for i in tmpChip:
            if i in self.Chip:
                self.Chip[i] += tmpChip[i] * (TurnoverRateT * A)
            else:
                self.Chip[i] = tmpChip[i] * (TurnoverRateT * A)
        import copy
        self.ChipList[dateT] = copy.deepcopy(self.Chip)

    def calcu(self, dateT, highT, lowT, avgT, volT, TurnoverRateT, minD=0.01, flag=1, AC=1):
        if flag == 1:
            self.calcuSin(dateT, highT, lowT, avgT, volT, TurnoverRateT, A=AC, minD=minD)
        elif flag == 2:
            self.calcuJUN(dateT, highT, lowT, volT, TurnoverRateT, A=AC, minD=minD)

    def calcuChip(self, flag=1, AC=1):  # flag 使用哪个计算方式,    AC 衰减系数
        low = self.data['low']
        high = self.data['high']
        vol = self.data['volume']
        TurnoverRate = self.data['turnover']
        avg = (high + low) / 2
        date = self.data['date']

        for i in range(len(date)):
            # if i < 90:
            #     continue

            highT = high[i]
            lowT = low[i]
            volT = vol[i]
            TurnoverRateT = TurnoverRate[i]
            avgT = avg[i]
            dateT = date[i]
            # 东方财富的小数位要注意，兄弟萌。我不除100懵逼了
            self.calcu(dateT, highT, lowT, avgT, volT, TurnoverRateT / 100, flag=flag, AC=AC)

    # 计算winner
    def winner(self, p=None):
        Profit = []
        date = self.data['date']

        if p == None:  # 不输入默认close
            p = self.data['close']
            count = 0
            for i in self.ChipList:
                # 计算目前的比例

                Chip = self.ChipList[i]
                total = 0
                be = 0
                for i in Chip:
                    total += Chip[i]
                    if i < p[count]:
                        be += Chip[i]
                if total != 0:
                    bili = be / total
                else:
                    bili = 0
                count += 1
                Profit.append(bili)
        else:
            for i in self.ChipList:
                # 计算目前的比例
                Chip = self.ChipList[i]
                total = 0
                be = 0
                for i in Chip:
                    total += Chip[i]
                    if i < p:
                        be += Chip[i]
                if total != 0:
                    bili = be / total
                else:
                    bili = 0
                Profit.append(bili)

        # import matplotlib.pyplot as plt
        # plt.plot(date[len(date) - 200:-1], Profit[len(date) - 200:-1])
        # plt.show()
        return Profit

    def lwinner(self, N=5, p=None):
        data = copy.deepcopy(self.data)
        date = data['date']
        ans = []
        for i in range(len(date)):
            if i < N:
                ans.append(None)
                continue
            self.data = data[i - N:i]
            self.data.index = range(0, N)
            self.__init__()
            self.calcuChip()  # 使用默认计算方式
            a = self.winner(p)
            ans.append(a[-1])
        # import matplotlib.pyplot as plt
        # plt.plot(date[len(date) - 60:-1], ans[len(date) - 60:-1])
        # plt.show()
        self.data = data
        return ans

    def cost(self, N):
        date = self.data['date']
        N = N / 100  # 转换成百分比
        ans = []
        for i in self.ChipList:  # 我的ChipList本身就是有顺序的
            Chip = self.ChipList[i]
            ChipKey = sorted(Chip.keys())  # 排序
            total = 0  # 当前比例
            sumOf = 0  # 所有筹码的总和
            for j in Chip:
                sumOf += Chip[j]

            for j in ChipKey:
                tmp = Chip[j]
                tmp = tmp / sumOf
                total += tmp
                if total > N:
                    ans.append(j)
                    break
        # import matplotlib.pyplot as plt
        # plt.plot(date[len(date) - 1000:-1], ans[len(date) - 1000:-1])
        # plt.show()
        return ans


if __name__ == "__main__":
    #stock = '603993'
    #stock = '002532'
    stock = '600121'
    date_format = '%Y-%m-%d'
    d_format = '%Y%m%d'
    dd = datetime.now()
    cur_d = dd.strftime(d_format)
    print(f'今天日期 {cur_d}')
    start_date = dd - timedelta(days=256)
    end_date = dd - timedelta(days=1)
    # ts初始化
    ts.set_token('d256364e28603e69dc6362aefb8eab76613b704035ee97b555ac79ab')
    ts_data = ts.pro_api()

    a = ChipDistribution()
    # a.get_data_from_csv()  # 获取数据
    a.get_data_from_tushare(stock, start_date, end_date)  # 获取数据
    a.calcuChip(flag=1, AC=1)  # 计算
    a.winner()  # 获利盘
    c = a.cost(90)  # 成本分布
    l = a.lwinner()
    r = dict()
    for i in range(len(c)):
        r[c[i]] = l[i]
    h_list = []
    l_list = []
    for i in sorted(r.keys()):
        # print(f'{i}\t{r[i]}')
        if r[i]:
            if i > a.price:
                h_list.append(r[i])
            else:
                l_list.append(r[i])
    h_sum = sum(h_list)
    l_sum = sum(l_list)
    print(h_sum)
    print(l_sum)
    # 计算获利比率
    h_per = h_sum / (h_sum + l_sum) * 100
    l_per = l_sum / (h_sum + l_sum) * 100
    print(f'获利比例 {l_per}%')

