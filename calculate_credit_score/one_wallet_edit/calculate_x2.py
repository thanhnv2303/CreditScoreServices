import csv
import time

from py2neo import Graph

from calculate_credit_score.one_wallet_edit.config import Neo4jConfig
from standardized_score_services import get_standardized_score_info

graph = Graph(Neo4jConfig.BOLT, auth=(Neo4jConfig.NEO4J_USERNAME, Neo4jConfig.NEO4J_PASSWORD))
wallets_data = graph.run(" MATCH (a:Wallet) return a.address, a.createdAt, "
                         "a.dailyTransactionAmounts,"
                         "a.dailyFrequencyOfTransactions,"
                         "a.numberOfLiquidation,"
                         "a.totalAmountOfLiquidation").data()

for i in range(len(wallets_data)):
    if wallets_data[i]['a.createdAt'] is None:
        wallets_data[i]['a.createdAt'] = time.time()
    if wallets_data[i]['a.dailyTransactionAmounts'] is None or wallets_data[i]['a.dailyTransactionAmounts'] == []:
        wallets_data[i]['a.dailyTransactionAmounts'] = [0 for _ in range(100)]
    if wallets_data[i]['a.dailyFrequencyOfTransactions'] is None or wallets_data[i][
        'a.dailyFrequencyOfTransactions'] == []:
        wallets_data[i]['a.dailyFrequencyOfTransactions'] = [0 for _ in range(100)]
    if wallets_data[i]['a.numberOfLiquidation'] is None:
        wallets_data[i]['a.numberOfLiquidation'] = 0
    if wallets_data[i]['a.totalAmountOfLiquidation'] is None:
        wallets_data[i]['a.totalAmountOfLiquidation'] = 0


class CalculateX21:
    def __init__(self):
        self.wallets_age = [(time.time() - x['a.createdAt']) for x in wallets_data]
        self.mns, self.sstd = get_standardized_score_info(self.wallets_age)

        self.t_score = {wallets_data[i]['a.address']: ((self.wallets_age[i] - self.mns) / self.sstd * 100 + 500) for i
                        in range(len(self.wallets_age))}

        with open('statisticreport.csv', 'r', newline='') as file:
            rows = list(csv.reader(file))
            for i in range(len(rows)):
                if rows[i] != [] and rows[0] == 'X21':
                    rows[i][1] = self.mns
                    rows[i][2] = self.sstd
        with open('statisticreport.csv', 'w', newline='') as file:
            writer = csv.writer(file)
            for row in rows:
                writer.writerow(row)


class CalculateX22:
    def __init__(self, k=100):
        self.sum_k_days = [sum(a['a.dailyTransactionAmounts'][0:k]) for a in wallets_data]
        self.mns, self.sstd = get_standardized_score_info(self.sum_k_days)

        self.t_score = {wallets_data[i]['a.address']: ((self.sum_k_days[i] - self.mns) / self.sstd * 100 + 500)
                        for i in range(len(self.sum_k_days))}

        with open('statisticreport.csv', 'r', newline='') as file:
            rows = list(csv.reader(file))
            for i in range(len(rows)):
                if rows[i] != [] and rows[0] == 'X22':
                    rows[i][1] = self.mns
                    rows[i][2] = self.sstd
        with open('statisticreport.csv', 'w', newline='') as file:
            writer = csv.writer(file)
            for row in rows:
                writer.writerow(row)


class CalculateX23:
    def __init__(self, k=100):
        self.sum_k_days = [sum(a['a.dailyFrequencyOfTransactions'][0:k]) for a in wallets_data]
        self.mns, self.sstd = get_standardized_score_info(self.sum_k_days)

        self.t_score = {wallets_data[i]['a.address']: ((self.sum_k_days[i] - self.mns) / self.sstd * 100 + 500)
                        for i in range(len(self.sum_k_days))}

        with open('statisticreport.csv', 'r', newline='') as file:
            rows = list(csv.reader(file))
            for i in range(len(rows)):
                if rows[i] != [] and rows[0] == 'X23':
                    rows[i][1] = self.mns
                    rows[i][2] = self.sstd
        with open('statisticreport.csv', 'w', newline='') as file:
            writer = csv.writer(file)
            for row in rows:
                writer.writerow(row)


class CalculateX24:
    def __init__(self):
        self.t_score = {}
        for i in range(len(wallets_data)):
            x = wallets_data[i]['a.numberOfLiquidation']
            if x >= 10:
                self.t_score[wallets_data[i]['a.address']] = 0
            else:
                self.t_score[wallets_data[i]['a.address']] = (x * (-100) + 1000)


class CalculateX25:
    def __init__(self):
        self.t_score = {}
        for i in range(len(wallets_data)):
            x = wallets_data[i]['a.totalAmountOfLiquidation']
            if x >= 10000:
                self.t_score[wallets_data[i]['a.address']] = 0
            else:
                self.t_score[wallets_data[i]['a.address']] = (x * (-0.1) + 1000)


class CalculateX2:
    def __init__(self):
        pass

    def get_x2_all(self):
        self.x21s = CalculateX21().t_score
        self.x22s = CalculateX22().t_score
        self.x23s = CalculateX23().t_score
        self.x24s = CalculateX24().t_score
        self.x25s = CalculateX25().t_score

        self.wallets = [wallet['a.address'] for wallet in wallets_data]

        self.x2s = {}
        for wallet in self.wallets:
            x21 = self.x21s[wallet]
            x22 = self.x22s[wallet]
            x23 = self.x23s[wallet]
            x24 = self.x24s[wallet]
            x25 = self.x25s[wallet]
            x2 = (0.3 * x21 + 0.2 * x22 + 0.2 * x23 + 0.1 * x24 + 0.2 * x25)
            if (x2 > 1000):
                self.x2s[wallet] = 1000
            else:
                self.x2s[wallet] = x2
        return self.x2s

    def get_x2_one(self, address: str):
        with open('statisticreport.csv', 'r', newline='') as file:
            rows = list(csv.reader(file))
            for i in range(len(rows)):
                if rows[i] != [] and rows[i][0] == 'X21':
                    x21_mns, x21_sstd = float(rows[i][1]), float(rows[i][2])
                if rows[i] != [] and rows[i][0] == 'X22':
                    x22_mns, x22_sstd = float(rows[i][1]), float(rows[i][2])
                if rows[i] != [] and rows[i][0] == 'X23':
                    x23_mns, x23_sstd = float(rows[i][1]), float(rows[i][2])

        for wallet in wallets_data:
            if wallet['a.address'] == address:
                x21 = ((time.time() - float(wallet['a.createdAt'])) - x21_mns) / x21_sstd * 100 + 500
                x22 = (sum(wallet['a.dailyTransactionAmounts'][0:100]) - x21_mns) / x21_sstd * 100 + 500
                x23 = (sum(wallet['a.dailyFrequencyOfTransactions'][0:100]) - x21_mns) / x21_sstd * 100 + 500
                numberOfLiquidation = wallet['a.numberOfLiquidation']
                totalAmountOfLiquidation = wallet['a.totalAmountOfLiquidation']
                if numberOfLiquidation >= 10:
                    x24 = 0
                else:
                    x24 = numberOfLiquidation * (-100) + 1000

                if totalAmountOfLiquidation >= 10000:
                    x25 = 0
                else:
                    x25 = totalAmountOfLiquidation * (-0.1) + 1000
                x2 = (0.3 * x21 + 0.2 * x22 + 0.2 * x23 + 0.1 * x24 + 0.2 * x25)

                if (x2 > 1000):
                    return 1000
                else:
                    return x2

        return 0

    def get_x2_list(self):
        return list(self.get_x2_all().values())


if __name__ == "__main__":
    calc = CalculateX2()
    print(calc.get_x2_one("0x1ca3Ac3686071be692be7f1FBeCd66864148v012V"))
