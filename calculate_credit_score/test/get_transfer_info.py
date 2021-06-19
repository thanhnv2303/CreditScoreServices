from py2neo import Graph
from config import Neo4jConfig

class GetTransferInfo():
    def __init__(self):
        bolt = Neo4jConfig.BOLT
        self._graph = Graph(bolt, auth=(Neo4jConfig.NEO4J_USERNAME, Neo4jConfig.NEO4J_PASSWORD))

    def neo4j_get_transfer_timestamp(self, transactionID):
        getter = self._graph.run("MATCH (m:Wallet)-[r:TRANSFER { transactionID: $transactionID}]->(n:Wallet) "
                                "return r.timestamp ",
                                transactionID = transactionID).data()
        return getter[0]["r.timestamp"]

    def neo4j_get_transfer_fromWallet(self, transactionID):
        getter = self._graph.run("MATCH (m:Wallet)-[r:TRANSFER { transactionID: $transactionID}]->(n:Wallet) "
                                "return r.fromWallet ",
                                transactionID = transactionID).data()
        return getter[0]["r.fromWallet"]

    def neo4j_get_transfer_toWallet(self, transactionID):
        getter = self._graph.run("MATCH (m:Wallet)-[r:TRANSFER { transactionID: $transactionID}]->(n:Wallet) "
                                "return r.toWallet ",
                                transactionID = transactionID).data()
        return getter[0]["r.toWallet"]

    def neo4j_get_transfer_token(self, transactionID):
        getter = self._graph.run("MATCH (m:Wallet)-[r:TRANSFER { transactionID: $transactionID}]->(n:Wallet) "
                                "return r.token ",
                                transactionID = transactionID).data()
        return getter[0]["r.token"]

    def neo4j_get_transfer_value(self, transactionID):
        getter = self._graph.run("MATCH (m:Wallet)-[r:TRANSFER { transactionID: $transactionID}]->(n:Wallet) "
                                "return r.value ",
                                transactionID = transactionID).data()
        return getter[0]['r.value']
    
if __name__ == "__main__":
    getter = GetTransferInfo()
    res = getter.neo4j_get_transfer_fromWallet("0x361ef5bcffb1c5eda4f25e16431e1bbb")
    print(res)
