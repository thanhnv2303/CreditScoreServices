import os
import sys

TOP_DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.insert(0, os.path.join(TOP_DIR, '../'))

from config.config import Neo4jConfig
from py2neo import Graph
import time
import csv


def get_property(property, getter):
    if property in getter:
        return getter[property]
    else:
        return 0


def get_tscore(value, mean, std):
    z_score = (float(value) - float(mean)) / float(std)
    t_score = z_score * 100 + 500
    if (t_score > 1000):
        return 1000
    return t_score


def calculate_average_second(values, timestamps, time_current):
    if values == 0:
        return 0
    if (len(values) == 1):
        if time_current == timestamps[0]:
            return 0
        else:
            return (values[0] / (time_current - timestamps[0]))
    sum = 0
    for i in range(len(values) - 1):
        sum += values[i] * (timestamps[i + 1] - timestamps[i])
    sum += values[-1] * (time_current - timestamps[-1])
    total_time = time_current - timestamps[0]
    average = sum / total_time
    return average


class CalculateCreditScoreAllWallet:

    def __init__(self):
        self.k = 100
        # get wallet data from KG
        bolt = f"bolt://{Neo4jConfig.HOST}:{Neo4jConfig.BOTH_PORT}"
        self.graph = Graph(bolt, auth=(Neo4jConfig.NEO4J_USERNAME, Neo4jConfig.NEO4J_PASSWORD))
        try:
            self.graph.run("Match () Return 1 Limit 1")
            print('Neo4j Database is connected')
        except Exception:
            print('Neo4j Database is not connected')
        self.getter = self.graph.run("match (w:Wallet) return w;").data()
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

    def calculate_x2(self, i: int):
        get_property('createdAt', self.getter[i]['w'])
        createdAt = get_property('createdAt', self.getter[i]['w'])
        numberOfLiquidation = get_property('numberOfLiquidation', self.getter[i]['w'])
        totalAmountOfLiquidation = get_property('totalAmountOfLiquidation', self.getter[i]['w'])
        dailyFrequencyOfTransactions = get_property('dailyFrequencyOfTransactions', self.getter[i]['w'])
        dailyTransactionAmounts = get_property('dailyTransactionAmounts', self.getter[i]['w'])
        age = self.time - createdAt
        # x21 - age of account
        x21 = get_tscore(age, self.age_of_account_statistic['mean'], self.age_of_account_statistic['std'])

        # x22 - transaction amount -
        if (dailyTransactionAmounts == 0):
            dailyTransactionAmounts_temp = 0
        else:
            dailyTransactionAmounts_temp = sum(dailyTransactionAmounts)
        x22 = get_tscore(dailyTransactionAmounts_temp, self.transaction_amount_statistic['mean'],
                         self.transaction_amount_statistic['std'])
        # x23 - frequency of transaction
        if (dailyFrequencyOfTransactions == []):
            x23 = 0
        else:
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
        x2 = 0.3 * x21 + 0.2 * x22 + 0.2 * x23 + 0.1 * x24 + 0.2 * x25
        return x2

    def calculate_x5(self, i: int):
        return 0
        tokens = get_property('tokens', self.getter[i]['w'])
        maxCreditToken = 0
        for i in range(len(tokens)):
            if tokens[i] in self.tokenCreditScore.keys():
                creditToken = self.tokenCreditScore[tokens[i]]
                if creditToken > maxCreditToken:
                    maxCreditToken = creditToken
        if (maxCreditToken > 1000):
            maxCreditToken = 1000
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
        loan_average = calculate_average_second(borrowChangeLogValues, borrowChangeLogTimestamps, self.time)
        balance_average = calculate_average_second(balanceChangeLogValues, balanceChangeLogTimestamps, self.time)
        deposit_average = calculate_average_second(depositChangeLogValues, depositChangeLogTimestamps, self.time)
        total_asset_average = balance_average + deposit_average - loan_average

        x12 = get_tscore(total_asset_average, self.total_assets_statistic['mean'],
                         self.total_assets_statistic['std'])
        # print('x12', total_asset_average)
        x1 = 0.04 * x11 + 0.96 * x12

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
        x3 = 0.6 * x31 + 0.4 * x32

        # x4 - Circulating asset
        if (total_asset_average == 0):
            x41 = 0
        else:
            ratio41 = deposit_average / total_asset_average
            x41 = 1000 * ratio41
        # print('deposit_average', deposit_average)
        # print('x41', x41)
        x4 = x41
        return x1, x3, x4

    def updateCreditScore(self, i: int, credit_score):
        # print(credit_score)
        address = get_property('address', self.getter[i]['w'])
        self.result = self.graph.run(
            "MATCH (a:Wallet { address: $address }) SET a.creditScore = $creditScore RETURN a.creditScore", \
            address=address, creditScore=credit_score)
        # print(i, credit_score)
        return 0

    def calculate_credit_score(self):
        for i in range(len(self.getter)):
            x2 = self.calculate_x2(i)
            [x1, x3, x4] = self.calculate_x134(i)
            x5 = self.calculate_x5(i)
            credit_score = 0.25 * x1 + 0.35 * x2 + 0.15 * x3 + 0.2 * x4 + 0.05 * x5
            self.updateCreditScore(i, credit_score)


if __name__ == '__main__':
    calc = CalculateCreditScoreAllWallet()
    calc.calculate_credit_score()
    pass
