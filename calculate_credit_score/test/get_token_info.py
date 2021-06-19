from py2neo import Graph
from config import Neo4jConfig

class GetTokenInfo():
    def __init__(self):
        bolt = Neo4jConfig.BOLT
        self._graph = Graph(bolt, auth=(Neo4jConfig.NEO4J_USERNAME, Neo4jConfig.NEO4J_PASSWORD))

    def neo4j_get_token_totalSupply(self, address):
        getter = self._graph.run("match (p:Token { address: $address }) return p.totalSupply ", address = address).data()
        return getter[0]["p.totalSupply"]

    def neo4j_get_token_symbol(self, address):
        getter = self._graph.run("match (p:Token { address: $address }) return p.symbol ", address = address).data()
        return getter[0]["p.symbol"]

    def neo4j_get_token_name(self, address):
        getter = self._graph.run("match (p:Token { address: $address }) return p.name ", address = address).data()
        return getter[0]["p.name"]

    def neo4j_get_token_decimal(self, address):
        getter = self._graph.run("match (p:Token { address: $address }) return p.decimal ", address = address).data()
        return getter[0]["p.decimal"]

    def neo4j_get_token_dailyFrequencyOfTransactions(self, address):
        getter = self._graph.run("match (p:Token { address: $address }) return p.dailyFrequencyOfTransactions ", address = address).data()
        return getter[0]["p.dailyFrequencyOfTransactions"]

    def neo4j_get_token_price(self, address):
        getter = self._graph.run("match (p:Token { address: $address }) return p.price ", address = address).data()
        return getter[0]["p.price"]

    def neo4j_get_token_creditScore(self, address):
        getter = self._graph.run("match (p:Token { address: $address }) return p.creditScore ", address = address).data()
        return getter[0]["p.creditScore"]

    def neo4j_get_token_highestPrice(self, address):
        getter = self._graph.run("match (p:Token { address: $address }) return p.highestPrice ", address = address).data()
        return getter[0]["p.highestPrice"]

    def neo4j_get_token_marketCap(self, address):
        getter = self._graph.run("match (p:Token { address: $address }) return p.marketCap ", address = address).data()
        return getter[0]["p.marketCap"]

    def neo4j_get_token_tradingVolume24(self, address):
        getter = self._graph.run("match (p:Token { address: $address }) return p.tradingVolume24 ", address = address).data()
        return getter[0]["p.tradingVolume24"]

    def neo4j_get_token_lastUpdatedAt(self, address):
        getter = self._graph.run("match (p:Token { address: $address }) return p.lastUpdatedAt ", address = address).data()
        return getter[0]["p.lastUpdatedAt"]
