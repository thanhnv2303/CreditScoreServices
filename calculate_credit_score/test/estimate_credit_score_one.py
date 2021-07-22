import os
import sys

TOP_DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.insert(0, os.path.join(TOP_DIR, '../'))

from config.config import Neo4jConfig
from py2neo import Graph
import time
from datetime import datetime
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
    if property in getter[0]['w']:
        return getter[0]['w'][property]
    else:
        return 0


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

class EstimateCreditScore:
    def __init__(self, address: str, type_transaction: int, amount: float, time_estimate: int):
        self.address = address
        self.k = 30
        self.h = 10
        self.type_transaction = type_transaction
        self.amount = amount
        # get wallet data from KG
        bolt = f"bolt://{Neo4jConfig.HOST}:{Neo4jConfig.BOTH_PORT}"
        self.graph = Graph(bolt, auth=(Neo4jConfig.NEO4J_USERNAME, Neo4jConfig.NEO4J_PASSWORD))
        try:
            self.graph.run("Match () Return 1 Limit 1")
            print('Neo4j Database is connected')
        except Exception:
            print('Neo4j Database is not connected')
        self.getter = self.graph.run("match (w:Wallet { address: $address }) return w;", address=address).data()
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

        self.timeCurrent = int(time.time())
        self.timeFuture = self.timeCurrent + time_estimate
        self.timestamps_chosen = self.timeFuture - self.k*86400

    def calculate_x2(self):
        createdAt = get_property('createdAt', self.getter)
        if(createdAt == 0):
            createdAt = self.timeCurrent
        numberOfLiquidation = get_property('numberOfLiquidation', self.getter)
        totalAmountOfLiquidation = get_property('totalAmountOfLiquidation', self.getter)
        dailyFrequencyOfTransactions = get_property('dailyFrequencyOfTransactions', self.getter)
        dailyTransactionAmounts = get_property('dailyTransactionAmounts', self.getter)
        age = self.timeFuture - createdAt
        # x21 - age of account
        x21 = get_tscore(age, self.age_of_account_statistic['mean'], self.age_of_account_statistic['std'])

        # x22 - transaction amount
        if (dailyTransactionAmounts == 0):
            dailyTransactionAmounts_temp = self.amount
        else:
            dailyTransactionAmounts_temp = sum(dailyTransactionAmounts) + self.amount
        x22 = get_tscore(dailyTransactionAmounts_temp, self.transaction_amount_statistic['mean'],
                         self.transaction_amount_statistic['std'])

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
        return x2

    def calculate_x5(self):
        return 0
        tokens = get_property('tokens', self.getter)
        maxCreditToken = 0
        for i in range(len(tokens)):
            if tokens[i] in self.tokenCreditScore.keys():
                creditToken = self.tokenCreditScore[tokens[i]]
                if creditToken > maxCreditToken:
                    maxCreditToken = creditToken
        if (maxCreditToken > 1000):
            maxCreditToken = 1000
        return maxCreditToken

    def calculate_x134(self):
        balanceInUSD = get_property('balanceInUSD', self.getter)
        depositInUSD = get_property('depositInUSD', self.getter)
        borrowInUSD = get_property('borrowInUSD', self.getter)
        balanceChangeLogTimestamps = get_property('balanceChangeLogTimestamps', self.getter)
        balanceChangeLogValues = get_property('balanceChangeLogValues', self.getter)
        depositChangeLogTimestamps = get_property('depositChangeLogTimestamps', self.getter)
        depositChangeLogValues = get_property('depositChangeLogValues', self.getter)
        borrowChangeLogTimestamps = get_property('borrowChangeLogTimestamps', self.getter)
        borrowChangeLogValues = get_property('borrowChangeLogValues', self.getter)

        if (self.type_transaction == 1):  # deposit
            balanceInUSD = balanceInUSD - self.amount
            if balanceInUSD < 0:
                balanceInUSD = 0
            depositInUSD = depositInUSD + self.amount

            if (balanceChangeLogTimestamps == 0):
                balanceChangeLogTimestamps = [self.timeCurrent + 1]
                balanceChangeLogValues = [balanceInUSD]
            else:
                balanceChangeLogTimestamps.append(self.timeCurrent + 1)
                balanceChangeLogValues.append(balanceInUSD)

            if (depositChangeLogTimestamps == 0):
                depositChangeLogTimestamps = [self.timeCurrent + 1]
                depositChangeLogValues = [depositInUSD]
            else:
                depositChangeLogTimestamps.append(self.timeCurrent + 1)
                depositChangeLogValues.append(depositInUSD)
        elif (self.type_transaction == 2):  # borrow
            depositInUSD = depositInUSD - self.amount
            if depositInUSD < 0:
                depositInUSD = 0
            borrowInUSD = borrowInUSD + self.amount

            if (depositChangeLogTimestamps == 0):
                depositChangeLogTimestamps = [self.timeCurrent + 1]
                depositChangeLogValues = [depositInUSD]
            else:
                depositChangeLogTimestamps.append(self.timeCurrent + 1)
                depositChangeLogValues.append(depositInUSD)
            if (borrowChangeLogTimestamps == 0):
                borrowChangeLogTimestamps = [self.timeCurrent + 1]
                borrowChangeLogValues = [borrowInUSD]
            else:
                borrowChangeLogTimestamps.append(self.timeCurrent + 1)
                borrowChangeLogValues.append(borrowInUSD)
        elif (self.type_transaction == 3):  # repay
            balanceInUSD = balanceInUSD - self.amount
            if balanceInUSD < 0:
                balanceInUSD = 0
            borrowInUSD = borrowInUSD - self.amount
            if borrowInUSD < 0:
                borrowInUSD = 0

            if (balanceChangeLogTimestamps == 0):
                balanceChangeLogTimestamps = [self.timeCurrent + 1]
                balanceChangeLogValues = [balanceInUSD]
            else:
                balanceChangeLogTimestamps.append(self.timeCurrent + 1)
                balanceChangeLogValues.append(balanceInUSD)

            if (borrowChangeLogTimestamps == 0):
                borrowChangeLogTimestamps = [self.timeCurrent + 1]
                borrowChangeLogValues = [borrowInUSD]
            else:
                borrowChangeLogTimestamps.append(self.timeCurrent + 1)
                borrowChangeLogValues.append(borrowInUSD)
        elif (self.type_transaction == 4):  # withdraw
            balanceInUSD = balanceInUSD + self.amount
            depositInUSD = depositInUSD - self.amount
            if depositInUSD < 0:
                depositInUSD = 0

            if (balanceChangeLogTimestamps == 0):
                balanceChangeLogTimestamps = [self.timeCurrent + 1]
                balanceChangeLogValues = [balanceInUSD]
            else:
                balanceChangeLogTimestamps.append(self.timeCurrent + 1)
                balanceChangeLogValues.append(balanceInUSD)

            if (depositChangeLogTimestamps == 0):
                depositChangeLogTimestamps = [self.timeCurrent + 1]
                depositChangeLogValues = [depositInUSD]
            else:
                depositChangeLogTimestamps.append(self.timeCurrent + 1)
                depositChangeLogValues.append(depositInUSD)


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
        loan_average = calculate_average_second(borrowChangeLogValues, borrowChangeLogTimestamps, self.timeFuture, self.timestamps_chosen)
        balance_average = calculate_average_second(balanceChangeLogValues, balanceChangeLogTimestamps, self.timeFuture, self.timestamps_chosen)
        deposit_average = calculate_average_second(depositChangeLogValues, depositChangeLogTimestamps, self.timeFuture, self.timestamps_chosen)
        total_asset_average = balance_average + deposit_average - loan_average

        x12 = get_tscore(total_asset_average, self.total_assets_statistic['mean'], self.total_assets_statistic['std'])
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

        # x4 - Circulating asset
        # x41 - invesment to total asset ratio
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
                if (depositChangeLogTimestamps[i] > (self.timeFuture - timeLimit)):
                    continue
                if (depositChangeLogValues[i] != depositChangeLogTimestamps[i + 1]):
                    if depositChangeLogTimestamps[i] in balanceChangeLogTimestamps:
                        j = balanceChangeLogTimestamps.index(depositChangeLogTimestamps[i])
                        if j < len(balanceChangeLogTimestamps) - 1:
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

        return x1, x3, x4

    def calculate_credit_score(self):
        x2 = self.calculate_x2()
        [x1, x3, x4] = self.calculate_x134()
        x5 = self.calculate_x5()
        credit_score = c1 * x1 + c2 * x2 + c3 * x3 + c4 * x4 + c5 * x5   
        return int(credit_score)

    def newCreditScore(self):
        if (self.getter == []):
            print("No Wallet")
            return 0
        credit_score = self.calculate_credit_score()
        print(
            " Credit Score will be " + str(credit_score) + " at " + datetime.utcfromtimestamp(self.timeFuture).strftime(
                '%Y-%m-%d %H:%M:%S'))
        return credit_score


if __name__ == '__main__':
    # EstimateCreditScore(address, type_transaction, time_estimates)
    # - address of wallet
    # - type_transaction: 1-deposit, 2-borrow, 3-repay, 4-withdraw
    # - amount[USD]
    # - time_estimates[seconds]
    calc = EstimateCreditScore('0x42ff331afdfe064c3e17fcf4486e13a885d3d1a7', 1, 100000, 86400)
    calc.newCreditScore()
    pass
