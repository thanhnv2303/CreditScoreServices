from py2neo import Graph

from calculate_credit_score.one_wallet_edit.config import Neo4jConfig

graph = Graph(Neo4jConfig.BOLT, auth=(Neo4jConfig.NEO4J_USERNAME, Neo4jConfig.NEO4J_PASSWORD))

tokens = graph.run("MATCH (a:Token) RETURN a.address, a.creditScore").data()
for i in range(len(tokens)):
    if tokens[i]['a.creditScore'] is None:
        tokens[i]['a.creditScore'] = 0


class CalculateX5:
    def __init__(self):
        pass

    def get_token_score_for_all(self):
        self.wallets = graph.run(" MATCH (a:Wallet) RETURN a.address, a.tokens;").data()
        self.tokens = {tokens[i]['a.address']: tokens[i]['a.creditScore'] for i in range(len(tokens))}

        self.tokenScores = {}
        for i in range(len(self.wallets)):
            if self.wallets[i]['a.tokens'] is None:
                self.tokenScores[self.wallets[i]['a.address']] = 0
            else:
                self.tokenScores[self.wallets[i]['a.address']] = max(
                    [self.tokens[j] for j in self.wallets[i]['a.tokens']])
        return self.tokenScores

    def get_token_score_for_one(self, address: str):
        return 0
        self.wallet = graph.run(" MATCH (a:Wallet { address: $address }) RETURN a.address, a.tokens;",
                                address=address).data()
        self.tokens = {tokens[i]['a.address']: tokens[i]['a.creditScore'] for i in range(len(tokens))}

        if self.wallet[0]['a.tokens'] is None:
            return 0
        else:
            return max([self.tokens[j] for j in self.wallet[0]['a.tokens']])

    def get_x51_list(self):
        return list(self.get_token_score_for_all().values())

    def get_x51_one(self, address: str):
        return self.get_token_score_for_one(address)

# if __name__ == "__main__":
#     calc = CalculateX51()
#     print(calc.get_x51_one('0x1ca3Ac3686071be692be7f1FBeCd668641476D7e'))
