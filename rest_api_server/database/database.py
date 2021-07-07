import logging
import random

from py2neo import Graph

from config.config import Neo4jConfig

logger = logging.getLogger("Database rest ")


class Database(object):
    """Manages connection to  database_common and makes async queries
    """

    def __init__(self):
        self._conn = None

        bolt = f"bolt://{Neo4jConfig.HOST}:{Neo4jConfig.BOTH_PORT}"
        self._graph = Graph(bolt, auth=(Neo4jConfig.NEO4J_USERNAME, Neo4jConfig.NEO4J_PASSWORD))

    def connect(self):
        return True

    def disconnect(self):
        pass

    def get_credit_score(self, wallet_address):
        credit_score = random.uniform(50.0, 99.9)
        getter = self._graph.run("match (p:Wallet { address: $address }) return p.creditScore ",
                                 address=wallet_address).data()
        if getter and getter[0]["p.creditScore"]:
            credit_score = getter[0]["p.creditScore"]
        else:
            self.set_credit_score(wallet_address, credit_score)
        return credit_score

    def get_wallet(self, wallet_address):
        getter = self._graph.run("match (p:Wallet { address: $address }) return p ",
                                 address=wallet_address).data()
        logger.info(getter)
        return getter

    def set_credit_score(self, wallet_address, credit_score):
        match = self._graph.run("MATCH (p:Wallet { address : $address}) return p ", address=wallet_address).data()
        if not match:
            create = self._graph.run("MERGE (p:Wallet { address: $address }) "
                                     "SET p.creditScore = $creditScore "
                                     "RETURN p",
                                     address=wallet_address, creditScore=credit_score).data()
        else:
            create = self._graph.run("MATCH (p:Wallet { address: $address }) "
                                     "SET p.creditScore = $creditScore "
                                     "RETURN p",
                                     address=wallet_address, creditScore=credit_score).data()

        return create[0]["p"]

    def update_credit_score(self, wallet_address):
        change = random.uniform(-5.5, 5.5)
        credit_score = self.get_credit_score(wallet_address)
        credit_score += change
        self.set_credit_score(wallet_address, credit_score)
        return credit_score

    def get_token_score(self, token_address):
        credit_score = random.uniform(60.0, 499.9)
        getter = self._graph.run("match (p:Token { address: $address }) return p.creditScore ",
                                 address=token_address).data()
        if getter and getter[0]["p.creditScore"]:
            credit_score = getter[0]["p.creditScore"] + random.uniform(-0.00002, 0.00002)
        else:
            self.set_credit_score(token_address, credit_score)
        return credit_score

    def set_token_credit_score(self, token_address, credit_score):
        match = self._graph.run("MATCH (p:Token { address : $address}) return p ", address=token_address).data()
        if not match:
            create = self._graph.run("MERGE (p:Token { address: $address }) "
                                     "SET p.creditScore = $creditScore "
                                     "RETURN p",
                                     address=token_address, creditScore=credit_score).data()
        else:
            create = self._graph.run("MATCH (p:Token { address: $address }) "
                                     "SET p.creditScore = $creditScore "
                                     "RETURN p",
                                     address=token_address, creditScore=credit_score).data()

        return create[0]["p"]

    def get_daily_frequency_of_transaction(self, address):
        frequency_of_transaction = []
        frequency_of_transaction_timestamp = []

        getter = self._graph.run(
            """
            match (p:Wallet { address: $address }) 
            return  p.dailyFrequencyOfTransactions ,
                    q.dailyFrequencyOfTransactionsTimestamp
            """,
            address=address).data()
        if getter and getter[0]["p.dailyFrequencyOfTransactions"]:
            frequency_of_transaction = getter[0]["p.dailyFrequencyOfTransactions"]
            frequency_of_transaction_timestamp = getter[0]["p.dailyFrequencyOfTransactionsTimestamp"]

        return frequency_of_transaction, frequency_of_transaction_timestamp
