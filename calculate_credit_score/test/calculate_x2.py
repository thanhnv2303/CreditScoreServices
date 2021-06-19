import time

from py2neo import Graph

from calculate_credit_score.test.standardized_score_services import get_standardized_score_info
from config.config import Neo4jConfig

graph = Graph(Neo4jConfig.BOLT, auth=(Neo4jConfig.NEO4J_USERNAME, Neo4jConfig.NEO4J_PASSWORD))


class CalculateX21:
    def __init__(self):
        self.data = graph.run(" MATCH (a:Wallet) return a.address, a.createdAt;").data()

        self.wallets_age = [(time.time() - x['a.createdAt']) for x in self.data if x['a.createdAt']]
        self.mns, self.sstd = get_standardized_score_info(self.wallets_age)

        self.t_score = {self.data[i]['a.address']: ((self.wallets_age[i] - self.mns) / self.sstd * 100 + 500) for i in
                        range(len(self.wallets_age))}


class CalculateX22:
    def __init__(self, k=100):
        self.data = graph.run(" MATCH (a:Wallet) return a.address, a.dailyTransactionAmounts;").data()

        self.sum_k_days = [sum(a['a.dailyTransactionAmounts'][0:k]) for a in self.data if
                           a['a.dailyTransactionAmounts']]
        self.mns, self.sstd = get_standardized_score_info(self.sum_k_days)

        self.t_score = {self.data[i]['a.address']: ((self.sum_k_days[i] - self.mns) / self.sstd * 100 + 500)
                        for i in range(len(self.sum_k_days))}


class CalculateX23:
    def __init__(self, k=100):
        self.data = graph.run(" MATCH (a:Wallet) return a.address, a.dailyFrequencyOfTransactions;").data()

        self.sum_k_days = [sum(a['a.dailyFrequencyOfTransactions'][0:k]) for a in self.data if
                           a['a.dailyFrequencyOfTransactions']]
        self.mns, self.sstd = get_standardized_score_info(self.sum_k_days)

        self.t_score = {self.data[i]['a.address']: ((self.sum_k_days[i] - self.mns) / self.sstd * 100 + 500)
                        for i in range(len(self.sum_k_days))}


class CalculateX24:
    def __init__(self):
        self.data = graph.run(" MATCH (a:Wallet) return a.address, a.numberOfLiquidation;").data()

        self.numbers_of_liquidation = [a['a.numberOfLiquidation'] for a in self.data if a['a.numberOfLiquidation']]
        self.mns, self.sstd = get_standardized_score_info(self.numbers_of_liquidation)

        self.t_score = {self.data[i]['a.address']: ((self.numbers_of_liquidation[i] - self.mns) / self.sstd * 100 + 500)
                        for i in range(len(self.numbers_of_liquidation))}


class CalculateX25:
    def __init__(self):
        self.data = graph.run(" MATCH (a:Wallet) return a.address, a.totalAmountOfLiquidation;").data()

        self.total_amounts_liquidation = [a['a.totalAmountOfLiquidation'] for a in self.data if
                                          a['a.totalAmountOfLiquidation']]
        self.mns, self.sstd = get_standardized_score_info(self.total_amounts_liquidation)

        self.t_score = {
            self.data[i]['a.address']: ((self.total_amounts_liquidation[i] - self.mns) / self.sstd * 100 + 500)
            for i in range(len(self.total_amounts_liquidation))}


class CalculateX2:
    def __init__(self):
        self.x21s = CalculateX21().t_score
        self.x22s = CalculateX22().t_score
        self.x23s = CalculateX23().t_score
        self.x24s = CalculateX24().t_score
        self.x25s = CalculateX25().t_score

        self.wallets = graph.run(" MATCH (a:Wallet) return a.address;").data()
        self.wallets = [wallet['a.address'] for wallet in self.wallets]

        self.x2s = {}
        for wallet in self.wallets:
            x21 = self.x21s.get(wallet) or 0
            x22 = self.x22s.get(wallet) or 0
            x23 = self.x23s.get(wallet) or 0
            x24 = self.x24s.get(wallet) or 0
            x25 = self.x25s.get(wallet) or 0

            x2 = 0.35 * (0.3 * x21 + 0.2 * x22 + 0.2 * x23 + 0.1 * x24 + 0.2 * x25)
            self.x2s[wallet] = x2

    def get_x2_list(self):
        return list(self.x2s.values())


if __name__ == "__main__":
    calc = CalculateX2()
    print(calc.x2s)
