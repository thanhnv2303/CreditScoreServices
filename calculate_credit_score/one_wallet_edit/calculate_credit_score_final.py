from py2neo import Graph

from calculate_credit_score.one_wallet_edit.calculate_one_wallet_x134 import *
from calculate_credit_score.one_wallet_edit.calculate_x2 import *
from calculate_credit_score.one_wallet_edit.calculate_x5 import *
from calculate_credit_score.one_wallet_edit.config import Neo4jConfig

graph = Graph(Neo4jConfig.BOLT, auth=(Neo4jConfig.NEO4J_USERNAME, Neo4jConfig.NEO4J_PASSWORD))


class CalculateCreditScoreOneWallet:
    def __init__(self, address: str):
        self.x2_calculator = CalculateX2()
        self.x5_calculator = CalculateX5()
        one_wallet = CalculateOneWallet(address)
        x1 = one_wallet.get_x1()
        x12 = one_wallet.get_x12()
        x3 = one_wallet.get_x3()
        x4 = one_wallet.get_x4()
        x2 = self.x2_calculator.get_x2_one(address)
        x5 = self.x5_calculator.get_x51_one(address)
        creditScore = 0.25 * (0.96*x1 + 0.04*x12) + 0.35 * x2 + 0.15 * x3 + 0.2 * x4 + 0.05 * x5
        if (creditScore > 1000):
            creditScore = 1000

        self.result = graph.run(
            "MATCH (a:Wallet { address: $address }) SET a.creditScore = $creditScore RETURN a.creditScore",
            address=address, creditScore=creditScore)
        print(creditScore)


if __name__ == "__main__":
    calc = CalculateCreditScoreOneWallet("0x42ff331afdfe064c3e17fcf4486e13a885d3d1a7")
    pass
