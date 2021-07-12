import logging
import time

from pymongo import MongoClient

from config.config import MongoDBConfig
from config.constant import TransactionConstant, BlockConstant, WalletConstant, TokenConstant, MongoIndexConstant
from config.performance_constant import PerformanceConstant
from database_common.memory_storage_test_performance import MemoryStoragePerformance

logger = logging.getLogger("IntermediaryDatabase")


class IntermediaryDatabase(object):
    """Manages connection to  database_common and makes async queries
    """

    def __init__(self):
        self._conn = None
        url = f"mongodb://{MongoDBConfig.NAME}:{MongoDBConfig.PASSWORD}@{MongoDBConfig.HOST}:{MongoDBConfig.PORT}"
        # logger.info("IntermediaryDatabase")
        # logger.info(url)
        self.mongo = MongoClient(url)
        self.mongo_db = self.mongo[MongoDBConfig.DATABASE]
        self.mongo_db = self.mongo[MongoDBConfig.DATABASE]
        self.mongo_transactions = self.mongo_db[MongoDBConfig.TRANSACTIONS]
        self.mongo_transactions_transfer = self.mongo_db[MongoDBConfig.TRANSACTIONS_TRANSFER]
        self.mongo_wallet = self.mongo_db[MongoDBConfig.WALLET]
        self.mongo_tokens = self.mongo_db[MongoDBConfig.TOKENS]
        self.mongo_blocks = self.mongo_db[MongoDBConfig.BLOCKS]

        self.mongo_token_collection_dict = {}
        self.performance_storage = MemoryStoragePerformance.getInstance()
        try:
            self._create_index()
        except Exception as e:
            logger.info(e)

    def _create_index(self):
        if MongoIndexConstant.tx_to_address not in self.mongo_transactions.index_information():
            self.mongo_transactions.create_index([("to_address", "hashed")], name=MongoIndexConstant.tx_to_address)
        if MongoIndexConstant.tx_from_address not in self.mongo_transactions.index_information():
            self.mongo_transactions.create_index([("from_address", "hashed")], name=MongoIndexConstant.tx_from_address)
        if MongoIndexConstant.tx_block_timestamp not in self.mongo_transactions.index_information():
            self.mongo_transactions.create_index([("block_timestamp", 1)], name=MongoIndexConstant.tx_block_timestamp)
        if MongoIndexConstant.blocks_number not in self.mongo_blocks.index_information():
            self.mongo_blocks.create_index([("number", "hashed")], name=MongoIndexConstant.blocks_number)

    def block_number_to_time_stamp(self, block_number):
        key = {BlockConstant.number: block_number}
        block = self.mongo_blocks.find_one(key)
        return block.get(BlockConstant.timestamp)

    def get_latest_block_update(self):
        start = time.time()
        latest_block = self.mongo_blocks.find_one(sort=[(BlockConstant.number, -1)])
        duration = time.time() - start
        self.performance_storage.accumulate_to_key(PerformanceConstant.get_latest_block_update, duration)
        # logger.info(f"Time to get latest block {time.time() - start}")
        return latest_block.get(BlockConstant.number) - 1

    def get_oldest_block_update(self):
        start = time.time()
        latest_block = self.mongo_blocks.find_one(sort=[(BlockConstant.number, 1)])
        duration = time.time() - start
        self.performance_storage.accumulate_to_key(PerformanceConstant.get_oldest_block_update, duration)
        # logger.info(f"time to get oldest block {time.time() - start}")
        return latest_block.get(BlockConstant.number) - 1

    def get_first_create_wallet(self, wallet_address):
        # key = {"$or": [
        #     {TransactionConstant.from_address: wallet_address},
        #     {TransactionConstant.to_address: wallet_address}
        # ]}
        key = {TransactionConstant.from_address: wallet_address}

        start = time.time()
        transaction = self.mongo_transactions.find_one(key, sort=[(TransactionConstant.block_timestamp, 1)])
        duration = time.time() - start
        self.performance_storage.accumulate_to_key(PerformanceConstant.get_first_create_wallet, duration)
        # logger.info(f"time to get first create wallet at transaction {time.time() - start}")
        return transaction.get(TransactionConstant.block_timestamp)

    def get_transfer_native_token_tx_in_block(self, block):
        key = {TransactionConstant.block_number: block}
        start = time.time()
        result = self.mongo_transactions_transfer.find(key)
        duration = time.time() - start
        self.performance_storage.accumulate_to_key(PerformanceConstant.get_transfer_native_token_tx_in_block, duration)
        # logger.info(f"tme to get_transfer_native_token_tx_in_block {time.time() - start} ")
        return result

    def get_events_at_of_smart_contract(self, block, smart_contract_address):
        smart_contract_address = str(smart_contract_address).lower()
        start = time.time()
        if not self.mongo_token_collection_dict.get(smart_contract_address):
            self.mongo_token_collection_dict[smart_contract_address] = self.mongo_db[smart_contract_address]
        key = {TransactionConstant.block_number: block}
        result = self.mongo_token_collection_dict.get(smart_contract_address).find(key)
        duration = time.time() - start
        self.performance_storage.accumulate_to_key(PerformanceConstant.get_events_at_of_smart_contract, duration)
        # logger.info(f"time to get_events_at_of_smart_contract {smart_contract_address} {time.time() - start} ")
        return result

    def get_wallet(self, wallet_address):
        key = {WalletConstant.address: wallet_address}
        start = time.time()
        result = self.mongo_wallet.find_one(key)
        duration = time.time() - start
        self.performance_storage.accumulate_to_key(PerformanceConstant.get_wallet, duration)
        # logger.info(f"Time to get wallet {time.time() - start}")
        return result

    def get_token(self, token_address):
        key = {TokenConstant.address: token_address}
        start = time.time()
        result = self.mongo_tokens.find_one(key)

        # logger.info(f"get_token {time.time() - start} ")
        return result

    def get_num_event_of_smart_contract_after_block(self, block_number, smart_contract_address):
        smart_contract_address = str(smart_contract_address).lower()
        start = time.time()
        if not self.mongo_token_collection_dict.get(smart_contract_address):
            self.mongo_token_collection_dict[smart_contract_address] = self.mongo_db[smart_contract_address]

        key = {
            TokenConstant.block_number: {"$gt": block_number}
        }

        result = self.mongo_token_collection_dict.get(smart_contract_address).find(key).count()
        duration = time.time() - start
        self.performance_storage.accumulate_to_key(PerformanceConstant.get_num_event_of_smart_contract_after_block,
                                                   duration)
        # logger.info(f"time to get_num_event_of_smart_contract_after_block {time.time() - start} ")
        return result
