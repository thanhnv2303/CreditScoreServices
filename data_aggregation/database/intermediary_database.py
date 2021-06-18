from pymongo import MongoClient

from config.config import MongoDBConfig


class IntermediaryDatabase(object):
    """Manages connection to  database_common and makes async queries
    """

    def __init__(self):
        self._conn = None
        # url = f"mongodb://{MongoDBConfig.NAME}:{MongoDBConfig.PASSWORD}@{MongoDBConfig.HOST}:{MongoDBConfig.PORT}"
        # self.mongo = MongoClient(url)
        # self.mongo_db = self.mongo[MongoDBConfig.DATABASE]

        # self._create_index()

    def update_statistic_credit(self, statistic_credit):
        pass

    def get_wallets_paging(self):
        return []
