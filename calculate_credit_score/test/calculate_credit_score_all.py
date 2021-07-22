import timeit
start = timeit.default_timer()
import os
import sys

TOP_DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.insert(0, os.path.join(TOP_DIR, '../'))

from config.config import Neo4jConfig
from py2neo import Graph
import time
import csv
c1 = 0.25
c11 = 0.04
c12 = 0.96
c2 = 0.35
c21 = 0.3
c22 = 0.2
c23 = 0.2
c24 = 0.1
c25 = 0.2
c3 = 0.15
c31 = 0.6
c32 = 0.4
c4 = 0.2
c41 = 0.4
c42 = 0.6
c5 = 0.05

def get_property(property, getter):
    if property in getter:
        return getter[property]
    else:
        return 0
def rotate(l, n):
    return l[n:] + l[:n]

def get_tscore(value, mean, std):
    z_score = (float(value) - float(mean)) / float(std)
    t_score = z_score * 100 + 500
    if (t_score > 1000):
        return 1000
    return t_score


def calculate_average_second(values, timestamps, time_current, timestamps_chosen):

    if values == 0:
        return 0
    if (len(values) == 1):
        if time_current == timestamps[0]:
            return 0
        else:
            return (values[0] / (time_current - timestamps[0]))
    d = dict(zip(timestamps, values))
    dictionary_items = d.items()
    sorted_items = sorted(dictionary_items)
    timestamps = []
    values = []

    for i in range(len(sorted_items)):
        if sorted_items[i][0] > timestamps_chosen:
            timestamps.append(sorted_items[i][0])
            values.append(sorted_items[i][1])
    if timestamps == []:
        return 0
    if len(timestamps) == 1:
        return values[0]
    sum = 0
    for i in range(len(values) - 1):
        temp = values[i] * (timestamps[i + 1] - timestamps[i])
        # print(temp)
        sum += values[i] * (timestamps[i + 1] - timestamps[i])
    sum += values[-1] * (time_current - timestamps[-1])
    total_time = time_current - timestamps[0]
    average = sum / total_time
    return average

def sumFrequency(array):
    if type(array) is not list:
        return array
    sum = 0
    for i in range(len(array)):
        if type(array[i]) != 'int':
            sum += 1
            continue
        sum += array[i]
    return sum

class CalculateCreditScoreAllWallet:

    def __init__(self):
        self.k = 30
        self.h = 10
        # get wallet data from KG
        bolt = f"bolt://{Neo4jConfig.HOST}:{Neo4jConfig.BOTH_PORT}"
        self.graph = Graph(bolt, auth=(Neo4jConfig.NEO4J_USERNAME, Neo4jConfig.NEO4J_PASSWORD))
        try:
            self.graph.run("Match () Return 1 Limit 1")
            print('Neo4j Database is connected')
        except Exception:
            print('Neo4j Database is not connected')
        self.getter = self.graph.run("match (w:Wallet) where not exists(w.holdOrai) return w;").data()
        # print(self.getter)

        # get list of token
        token_creditScore = self.graph.run("match (t:Token) return t.address, t.creditScore;").data()
        self.tokenCreditScore = {}
        for i in range(len(token_creditScore)):
            if token_creditScore[i]['t.creditScore'] is None:
                self.tokenCreditScore.update({token_creditScore[i]['t.address']: 0})
            else:
                self.tokenCreditScore.update({token_creditScore[i]['t.address']: token_creditScore[i]['t.creditScore']})
        # get statistic report
        with open('statistic_report.csv', mode='r') as file:

            # reading the CSV file
            csvFile = csv.DictReader(file)
            readlines = []
            for lines in csvFile:
                readlines.append(lines)

        self.total_assets_statistic = readlines[0]
        self.age_of_account_statistic = readlines[1]
        self.transaction_amount_statistic = readlines[2]
        self.frequency_of_transaction = readlines[3]

        self.time = int(time.time())
        self.timestamps_chosen = self.time - self.k*86400

        self.address = list()
        self.x11 = list()
        self.x12 = list()
        self.x21 = list()
        self.x22 = list()
        self.x23 = list()
        self.x24 = list()
        self.x25 = list()
        self.x31 = list()
        self.x32 = list()
        self.x41 = list()
        self.x42 = list()
        self.x51 = list()
        self.creditScore = list()

    def calculate_x2(self, i: int):
        createdAt = get_property('createdAt', self.getter[i]['w'])

        numberOfLiquidation = get_property('numberOfLiquidation', self.getter[i]['w'])
        totalAmountOfLiquidation = get_property('totalAmountOfLiquidation', self.getter[i]['w'])
        dailyFrequencyOfTransactions = get_property('dailyFrequencyOfTransactions', self.getter[i]['w'])
        dailyTransactionAmounts = get_property('dailyTransactionAmounts', self.getter[i]['w'])

        # x21 - age of account
        if (createdAt == 0):
            x21 = 0
        else:
            age = self.time - createdAt
            x21 = get_tscore(age, self.age_of_account_statistic['mean'], self.age_of_account_statistic['std'])
            if(x21 > 1000):
                x21 = 1000
        # x22 - transaction amount -
        if (dailyTransactionAmounts == 0):
            x22 = 0
        else:
            dailyTransactionAmounts_temp = sum(dailyTransactionAmounts)
            x22 = get_tscore(dailyTransactionAmounts_temp, self.transaction_amount_statistic['mean'],
                         self.transaction_amount_statistic['std'])
            if x22 > 1000:
                x22 = 1000
        # x23 - frequency of transaction
        if (dailyFrequencyOfTransactions == 0):
            x23 = 0
        else:
            x23 = get_tscore(sumFrequency(dailyFrequencyOfTransactions), self.frequency_of_transaction['mean'],
                             self.frequency_of_transaction['std'])
            if (x23 > 1000):
                x23 = 1000

        # x24 - number of liquidations
        if (numberOfLiquidation < 10):
            x24 = -100 * numberOfLiquidation + 1000
        else:
            x24 = 0

        # x25 - total value of liquidations
        if (totalAmountOfLiquidation < 0):
            print("error liquid amount")
        if (totalAmountOfLiquidation < 10000):
            x25 = 1000 - 0.1 * totalAmountOfLiquidation
        else:
            x25 = 0
        # x2 - Activity history
        x2 = c21 * x21 + c22 * x22 + c23 * x23 + c24 * x24 + c25 * x25
        x2_list = [x21, x22, x23, x24, x25]
        self.x21.append(x21)
        self.x22.append(x22)
        self.x23.append(x23)
        self.x24.append(x24)
        self.x25.append(x25)
        return x2, x2_list

    def calculate_x5(self, i: int):
        tokens = get_property('tokens', self.getter[i]['w'])
        if (tokens == 0):
            self.x51.append(0)
            return 0
        maxCreditToken = 0
        for i in range(len(tokens)):
            if tokens[i] in self.tokenCreditScore.keys():
                creditToken = self.tokenCreditScore[tokens[i]]
                if creditToken > maxCreditToken:
                    maxCreditToken = creditToken
        if (maxCreditToken > 1000):
            maxCreditToken = 1000
        self.x51.append(maxCreditToken)
        return maxCreditToken

    def calculate_x134(self, i: int):
        balanceInUSD = get_property('balanceInUSD', self.getter[i]['w'])
        depositInUSD = get_property('depositInUSD', self.getter[i]['w'])
        borrowInUSD = get_property('borrowInUSD', self.getter[i]['w'])
        balanceChangeLogTimestamps = get_property('balanceChangeLogTimestamps', self.getter[i]['w'])
        balanceChangeLogValues = get_property('balanceChangeLogValues', self.getter[i]['w'])
        depositChangeLogTimestamps = get_property('depositChangeLogTimestamps', self.getter[i]['w'])
        depositChangeLogValues = get_property('depositChangeLogValues', self.getter[i]['w'])
        borrowChangeLogTimestamps = get_property('borrowChangeLogTimestamps', self.getter[i]['w'])
        borrowChangeLogValues = get_property('borrowChangeLogValues', self.getter[i]['w'])
        # x1 - total asset
        x11 = balanceInUSD + depositInUSD - borrowInUSD
        if (x11 < 1000):
            x11 = 0
        elif (x11 < 10000):
            x11 = 0.1 * x11
        else:
            x11 = 1000
        # print('x11', x11)
        # x1 = 0
        # x3 = 0
        # x4 = 0
        loan_average = calculate_average_second(borrowChangeLogValues, borrowChangeLogTimestamps, self.time, self.timestamps_chosen)
        if loan_average == 0:
            loan_average = borrowInUSD
        balance_average = calculate_average_second(balanceChangeLogValues, balanceChangeLogTimestamps, self.time, self.timestamps_chosen)
        if balance_average == 0:
            balance_average = balanceInUSD
        deposit_average = calculate_average_second(depositChangeLogValues, depositChangeLogTimestamps, self.time, self.timestamps_chosen)
        if deposit_average == 0:
            deposit_average = depositInUSD
        total_asset_average = balance_average + deposit_average - loan_average
        if(total_asset_average <=0 ):
            x12 = 0
        else:
            x12 = get_tscore(total_asset_average, self.total_assets_statistic['mean'],
                         self.total_assets_statistic['std'])
            if x12 > 1000:
                x12 = 1000
        # print('x12', total_asset_average)
        x1 = c11 * x11 + c12 * x12

        # x3 - loan ratio
        if (balance_average == 0):
            x31 = 0
        else:
            ratio31 = loan_average / balance_average
            x31 = 1000 * (1 - min(1, ratio31))
        # print('x31',x31 )
        if (deposit_average == 0):
            x32 = 0
        else:
            ratio32 = loan_average / deposit_average
            x32 = 1000 * (1 - min(1, ratio32))

        # print('x32', x32)
        x3 = c31 * x31 + c32 * x32
        if x3 > 1000:
            x3 = 1000
        # x4 - Circulating asset
        # x41 - investment to total asset ratio
        if (total_asset_average == 0):
            x41 = 0
        else:
            ratio41 = deposit_average / total_asset_average
            x41 = 1000 * ratio41
            if x41 < 0:
                x41 = 0
            if x41 > 1000:
                x41 = 1000
        # x42 - Return on investment ROI
        if type(depositChangeLogTimestamps) is not list:
            x42 = 0
        else:
            timeLimit = 86400 * self.h
            return_on_investment = 0
            for i in range(len(depositChangeLogTimestamps) - 1):
                if (depositChangeLogTimestamps[i] > (self.time - timeLimit)):
                    continue
                if (depositChangeLogValues[i] != depositChangeLogTimestamps[i + 1]):
                    if depositChangeLogTimestamps[i] in balanceChangeLogTimestamps:
                        j = balanceChangeLogTimestamps.index(depositChangeLogTimestamps[i])
                        if j < len(balanceChangeLogTimestamps)-1:
                            if balanceChangeLogTimestamps[j + 1] == depositChangeLogTimestamps[i + 1]:
                                d0 = depositChangeLogValues[i]
                                d1 = depositChangeLogValues[i + 1]
                                b0 = balanceChangeLogValues[j]
                                b1 = balanceChangeLogValues[j + 1]
                                period_of_time = depositChangeLogTimestamps[i + 1] - depositChangeLogTimestamps[i]
                                if b1 == b0:
                                    continue
                                profit = b1 + d1 - b0 - d0
                                if d0 != 0:
                                    return_on_investment_temp = (profit / d0) * (period_of_time / timeLimit)
                                    return_on_investment += return_on_investment_temp

            if return_on_investment <= 0:
                x42 = 0
            elif return_on_investment > (self.h * 0.15 / 365):
                x42 = 1000
            else:
                x42 = (return_on_investment * 365 * 1000) / (self.h * 0.15)

                # print('deposit_average', deposit_average)
                # print('x41', x41)
        x4 = c41 * x41 + c42 * x42
        x1_list = [x11, x12]
        x3_list = [x31, x32]
        x4_list = [x41, x42]
        self.x11.append(x11)
        self.x12.append(x12)
        self.x31.append(x31)
        self.x32.append(x32)
        self.x41.append(x41)
        self.x42.append(x42)
        return x1, x3, x4, x1_list, x3_list, x4_list

    def updateCreditScore(self, i: int, credit_score):
        # print(credit_score)
        address = get_property('address', self.getter[i]['w'])
        creditScoreChangeLogTimestamps = get_property('creditScoreChangeLogTimestamps', self.getter[i]['w'])
        creditScoreChangeLogValues = get_propzerty('creditScoreChangeLogValues', self.getter[i]['w'])
        historyCreditScore = get_property('historyCreditScore', self.getter[i]['w'])
        if historyCreditScore == 0:
            historyCreditScore = [0]*30
        historyCreditScore = rotate(historyCreditScore, 1)  # delete 30th previous day
        historyCreditScore[29] = credit_score


        # print(creditScoreChangeLogTimestamps == 0)
        if (creditScoreChangeLogTimestamps == 0):
            creditScoreChangeLogTimestamps = [self.time]
            creditScoreChangeLogValues = [credit_score]
        else:
            creditScoreChangeLogTimestamps.append(self.time)
            creditScoreChangeLogValues.append(credit_score)
        self.result = self.graph.run(
            "MATCH (a:Wallet { address: $address }) SET a.creditScore = $creditScore, a.creditScoreChangeLogTimestamps = $ChangeLogTimestamps, a.creditScoreChangeLogValues = $ChangeLogValues, a.historyCreditScore = $historyCredit RETURN a.creditScore", \
            address=address, creditScore=credit_score, ChangeLogTimestamps=creditScoreChangeLogTimestamps, ChangeLogValues=creditScoreChangeLogValues, historyCredit = historyCreditScore)

        print(i, credit_score)
        return 0

    def calculate_credit_score(self):
        for i in range(len(self.getter)):
            [x2, x2_list] = self.calculate_x2(i)
            [x1, x3, x4, x1_list, x3_list, x4_list] = self.calculate_x134(i)
            x5 = self.calculate_x5(i)
            credit_score = c1 * x1 + c2 * x2 + c3 * x3 + c4 * x4 + c5 * x5
            address = get_property('address', self.getter[i]['w'])
            self.result = self.graph.run(
                "MATCH (a:Wallet { address: $address }) SET a.creditScorex1 = $x1list, a.creditScorex2 = $x2list, a.creditScorex3 = $x3list, a.creditScorex4 = $x4list, a.creditScorex5 = $x5 RETURN a.creditScore", \
                address=address, x1list=x1_list, x2list=x2_list, x3list=x3_list, x4list=x4_list, x5=x5)
            self.updateCreditScore(i, int(credit_score))
        rows = []
        for i in range(len(self.address)):
            row = {'Wallet': self.address[i],
                   'Credit Score': self.creditScore[i],
                   'x11': self.x11[i],
                   'x12': self.x12[i],
                   'x21': self.x21[i],
                   'x22': self.x22[i],
                   'x23': self.x23[i],
                   'x24': self.x24[i],
                   'x25': self.x25[i],
                   'x31': self.x31[i],
                   'x32': self.x32[i],
                   'x41': self.x41[i],
                   'x42': self.x42[i],
                   'x51': self.x51[i]
                   }
            # print(row)
            rows.append(row)
        # print(len(rows))
        ### Make csv file wallets
        fieldnames = ['Wallet', 'Credit Score', 'x11', 'x12', 'x21', 'x22', 'x23', 'x24', 'x25', 'x31', 'x32', 'x41',
                      'x42', 'x51']
        with open('statistic_bsc_orai.csv', 'w', encoding='UTF8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

if __name__ == '__main__':
    calc = CalculateCreditScoreAllWallet()
    calc.calculate_credit_score()
    stop = timeit.default_timer()

    print('Time: ', stop - start)
    pass
