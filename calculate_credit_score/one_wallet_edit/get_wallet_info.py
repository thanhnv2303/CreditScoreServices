from py2neo import Graph

from calculate_credit_score.one_wallet_edit.config import Neo4jConfig


class GetWalletInfo:
    def __init__(self):
        bolt = Neo4jConfig.BOLT
        self._graph = Graph(bolt, auth=(Neo4jConfig.NEO4J_USERNAME, Neo4jConfig.NEO4J_PASSWORD))

    def neo4j_get_wallet_lastUpdatedAt(self, address):
        getter = self._graph.run("match (p:Wallet { address: $address }) return p.lastUpdatedAt ",
                                 address=address).data()
        return getter[0]["p.lastUpdatedAt"]

    def neo4j_get_wallet_creditScore(self, address):
        getter = self._graph.run("match (p:Wallet { address: $address }) return p.creditScore ", address=address).data()
        return getter[0]["p.creditScore"]

    def neo4j_get_wallet_tokens(self, address):
        getter = self._graph.run("match (p:Wallet { address: $address }) return p.tokens ", address=address).data()
        return getter[0]["p.tokens"]

    def neo4j_get_wallet_tokenBalances(self, address):
        getter = self._graph.run("match (p:Wallet { address: $address }) return p.tokenBalances ",
                                 address=address).data()
        return getter[0]["p.tokenBalances"]

    def neo4j_get_wallet_balanceInUSD(self, address):
        getter = self._graph.run("match (p:Wallet { address: $address }) return p.balanceInUSD ",
                                 address=address).data()
        return getter[0]["p.balanceInUSD"]

    def neo4j_get_wallet_balanceChangeLogTimestamps(self, address):
        getter = self._graph.run("match (p:Wallet { address: $address }) return p.balanceChangeLogTimestamps ",
                                 address=address).data()
        return getter[0]["p.balanceChangeLogTimestamps"]

    def neo4j_get_wallet_balanceChangeLogValues(self, address):
        getter = self._graph.run("match (p:Wallet { address: $address }) return p.balanceChangeLogValues ",
                                 address=address).data()
        return getter[0]["p.balanceChangeLogValues"]

    def neo4j_get_wallet_createdAt(self, address):
        getter = self._graph.run("match (p:Wallet { address: $address }) return p.createdAt ", address=address).data()
        return getter[0]["p.createdAt"]

    def neo4j_get_wallet_depositTokens(self, address):
        getter = self._graph.run("match (p:Wallet { address: $address }) return p.depositTokens ",
                                 address=address).data()
        return getter[0]["p.depositTokens"]

    def neo4j_get_wallet_depositTokenBalances(self, address):
        getter = self._graph.run("match (p:Wallet { address: $address }) return p.depositTokenBalances ",
                                 address=address).data()
        return getter[0]["p.depositTokenBalances"]

    def neo4j_get_wallet_depositInUSD(self, address):
        getter = self._graph.run("match (p:Wallet { address: $address }) return p.depositInUSD ",
                                 address=address).data()
        return getter[0]["p.depositInUSD"]

    def neo4j_get_wallet_depositChangeLogTimestamps(self, address):
        getter = self._graph.run("match (p:Wallet { address: $address }) return p.depositChangeLogTimestamps ",
                                 address=address).data()
        return getter[0]["p.depositChangeLogTimestamps"]

    def neo4j_get_wallet_depositChangeLogValues(self, address):
        getter = self._graph.run("match (p:Wallet { address: $address }) return p.depositChangeLogValues ",
                                 address=address).data()
        return getter[0]["p.depositChangeLogValues"]

    def neo4j_get_wallet_borrowTokens(self, address):
        getter = self._graph.run("match (p:Wallet { address: $address }) return p.borrowTokens ",
                                 address=address).data()
        return getter[0]["p.borrowTokens"]

    def neo4j_get_wallet_borrowTokenBalances(self, address):
        getter = self._graph.run("match (p:Wallet { address: $address }) return p.borrowTokenBalances ",
                                 address=address).data()
        return getter[0]["p.borrowTokenBalances"]

    def neo4j_get_wallet_borrowInUSD(self, address):
        getter = self._graph.run("match (p:Wallet { address: $address }) return p.borrowInUSD ", address=address).data()
        return getter[0]["p.borrowInUSD"]

    def neo4j_get_wallet_borrowChangeLogTimestamps(self, address):
        getter = self._graph.run("match (p:Wallet { address: $address }) return p.borrowChangeLogTimestamps ",
                                 address=address).data()
        return getter[0]["p.borrowChangeLogTimestamps"]

    def neo4j_get_wallet_borrowChangeLogValues(self, address):
        getter = self._graph.run("match (p:Wallet { address: $address }) return p.borrowChangeLogValues ",
                                 address=address).data()
        return getter[0]["p.borrowChangeLogValues"]

    def neo4j_get_wallet_numberOfLiquidation(self, address):
        getter = self._graph.run("match (p:Wallet { address: $address }) return p.numberOfLiquidation ",
                                 address=address).data()
        return getter[0]["p.numberOfLiquidation"]

    def neo4j_get_wallet_totalAmountOfLiquidation(self, address):
        getter = self._graph.run("match (p:Wallet { address: $address }) return p.totalAmountOfLiquidation ",
                                 address=address).data()
        return getter[0]["p.totalAmountOfLiquidation"]

    def neo4j_get_wallet_dailyTransactionAmounts(self, address):
        getter = self._graph.run("match (p:Wallet { address: $address }) return p.dailyTransactionAmounts ",
                                 address=address).data()
        return getter[0]["p.dailyTransactionAmounts"]

    def neo4j_get_wallet_dailyFrequencyOfTransactions(self, address):
        getter = self._graph.run("match (p:Wallet { address: $address }) return p.dailyFrequencyOfTransactions ",
                                 address=address).data()
        return getter[0]["p.dailyFrequencyOfTransactions"]


if __name__ == "__main__":
    getter = GetWalletInfo()
    print(getter.neo4j_get_wallet_balanceInUSD("0x1ca3Ac3686071be692be7f1FBeCd668641476D7e"))
