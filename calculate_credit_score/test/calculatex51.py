import re
from config.config import Neo4jConfig
from py2neo import Graph

graph = Graph(Neo4jConfig.BOLT, auth=(Neo4jConfig.NEO4J_USERNAME, Neo4jConfig.NEO4J_PASSWORD))

class CalculateX51:
    def __init__(self):
        self.wallets = graph.run(" MATCH (a:Wallet) RETURN a.address, a.tokens;").data()
        self.tokens = graph.run("MATCH (a:Token) RETURN a.address, a.creditScore").data()
        self.tokens = {self.tokens[i]['a.address']:self.tokens[i]['a.creditScore'] for i in range(len(self.tokens))}
        
        self.tokenScores = {}
        for i in range(len(self.wallets)):
            self.tokenScores[self.wallets[i]['a.address']] = max([self.tokens[j] for j in self.wallets[i]['a.tokens']])
    def get_x51_list(self):
        return list(self.tokenScores.values())
if __name__ == "__main__":
    calc = CalculateX51()
    print(calc.tokenScores)
