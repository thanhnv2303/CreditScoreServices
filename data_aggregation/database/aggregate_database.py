from py2neo import Graph

from config.config import Neo4jConfig


class Database(object):
    """Manages connection to  database_common and makes async queries
    """

    def __init__(self):
        self._conn = None

        bolt = f"bolt://{Neo4jConfig.HOST}:{Neo4jConfig.BOTH_PORT}"
        self._graph = Graph(bolt, auth=(Neo4jConfig.NEO4J_USERNAME, Neo4jConfig.NEO4J_PASSWORD))

        # self._create_index()

    def update_statistic_credit(self, statistic_credit):
        pass

    def get_wallets_paging(self):
        return []
