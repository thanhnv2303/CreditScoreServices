from pymongo import MongoClient

from config.config import MongoDBConfig
from config.constant import TransactionConstant, BlockConstant, WalletConstant


class IntermediaryDatabase(object):
    """Manages connection to  database_common and makes async queries
    """

    def __init__(self):
        self._conn = None
        url = f"mongodb://{MongoDBConfig.NAME}:{MongoDBConfig.PASSWORD}@{MongoDBConfig.HOST}:{MongoDBConfig.PORT}"
        self.mongo = MongoClient(url)
        self.mongo_db = self.mongo[MongoDBConfig.DATABASE]
        self.mongo_db = self.mongo[MongoDBConfig.DATABASE]
        self.mongo_transactions = self.mongo_db[MongoDBConfig.TRANSACTIONS]
        self.mongo_transactions_transfer = self.mongo_db[MongoDBConfig.TRANSACTIONS_TRANSFER]
        self.mongo_wallet = self.mongo_db[MongoDBConfig.WALLET]
        self.mongo_tokens = self.mongo_db[MongoDBConfig.TOKENS]
        self.mongo_blocks = self.mongo_db[MongoDBConfig.BLOCKS]

        self.mongo_token_collection_dict = {}
        # self._create_index()

    def block_number_to_time_stamp(self, block_number):
        key = {BlockConstant.number: block_number}
        block = self.mongo_blocks.find_one(key)
        return block.get(BlockConstant.block_timestamp)

    def get_latest_block_update(self):
        latest_block = self.mongo_blocks.find_one(sort=[(BlockConstant.number, -1)])
        return latest_block.get(BlockConstant.number) - 1

    def get_oldest_block_update(self):
        latest_block = self.mongo_blocks.find_one(sort=[(BlockConstant.number, 1)])
        return latest_block.get(BlockConstant.number) - 1

    def get_first_create_wallet(self, wallet_address):
        key = {"$or": [
            {TransactionConstant.from_address: wallet_address},
            {TransactionConstant.to_address: wallet_address}
        ]}

        transaction = self.mongo_transactions.find_one(key, sort=[(TransactionConstant.block_timestamp, 1)])

        return transaction.get(TransactionConstant.block_timestamp)

    def get_transfer_native_token_tx_in_block(self, block):
        key = {TransactionConstant.block_number: block}
        return self.mongo_transactions_transfer.find(key)

    def get_events_at_of_smart_contract(self, block, smart_contract_address):
        str(smart_contract_address).lower()
        if not self.mongo_token_collection_dict.get(smart_contract_address):
            self.mongo_token_collection_dict[smart_contract_address] = self.mongo_db[smart_contract_address]
        key = {TransactionConstant.block_number: block}
        return self.mongo_token_collection_dict.get(smart_contract_address).find(key)

    def get_wallet(self, wallet_address):
        key = {WalletConstant.address: wallet_address}
        return self.mongo_wallet.find_one(key)
