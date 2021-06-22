# MIT License
#
# Copyright (c) 2018 Evgeny Medvedev, evge.medvedev@gmail.com
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
# import asyncio
import logging

from config.constant import LoggerConstant, TransactionConstant, WalletConstant, TokenConstant
from config.data_aggregation_constant import MemoryStorageKeyConstant
from data_aggregation.database.intermediary_database import IntermediaryDatabase
from data_aggregation.database.klg_database import KlgDatabase
from data_aggregation.database.relationships_model import Transfer
from data_aggregation.services.price_service import PriceService
from database_common.memory_storage import MemoryStorage
from executors.batch_work_executor import BatchWorkExecutor
from jobs.base_job import BaseJob

logger = logging.getLogger(LoggerConstant.AggregateNativeTokenTransferJob)


# Exports blocks and transactions
class AggregateNativeTokenTransferJob(BaseJob):
    def __init__(
            self,
            start_block,
            end_block,
            price_service=PriceService(),
            batch_size=4,
            max_workers=8,
            intermediary_database=IntermediaryDatabase(),
            klg_database=KlgDatabase()
    ):
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.batch_work_executor = BatchWorkExecutor(batch_size, max_workers)
        self.intermediary_database = intermediary_database
        self.klg_database = klg_database
        self.end_block = end_block
        self.start_block = start_block
        self.price_service = price_service

    def _start(self):
        local_storage = MemoryStorage.getInstance()
        self.update_wallet_storage: dict = local_storage.get_element(MemoryStorageKeyConstant.update_wallet)

    def _end(self):
        self.batch_work_executor.shutdown()

    def _export(self):
        self.batch_work_executor.execute(
            range(self.start_block, self.end_block + 1),
            self._export_batch,
            total_items=self.end_block - self.start_block + 1
        )

    def _export_batch(self, block_number_batch):
        for block_number in block_number_batch:
            self._export_data_in_transaction_transfer_native_token(block_number)

    def _export_data_in_transaction_transfer_native_token(self, block):
        txs = self.intermediary_database.get_transfer_native_token_tx_in_block(block)
        for tx in txs:
            related_wallets = tx.get(TransactionConstant.related_wallets)
            timestamp = tx.get(TransactionConstant.block_timestamp)
            # timestamp_day = round_timestamp_to_date(timestamp)
            timestamp_day = timestamp
            if not related_wallets:
                return
            for wallet in related_wallets:
                """
                 thêm thông tin địa chỉ ví vào kho để update sau cho các thông tin không cần lịch sử.
                """
                logger.info("wallet--------------------")
                logger.info(wallet)
                wallet_address = wallet.get(WalletConstant.address)

                wallet_in_storage = self.update_wallet_storage.get(wallet_address)
                if wallet_in_storage and wallet_in_storage.get(TransactionConstant.block_number) > wallet.get(
                        TransactionConstant.block_number):
                    pass
                else:
                    self.update_wallet_storage[wallet_address] = wallet

                """
                thêm dữ liệu biến động số dư vào trong node wallet trong knowledge graph
                """
                balance = wallet.get(WalletConstant.balance)
                balance_value = self.price_service.get_total_value(balance)
                balance_100 = self.klg_database.get_balance_100(wallet_address)
                balance_100[timestamp_day] = balance_value

                self.klg_database.update_balance_100(wallet_address, balance_100)
            """
            Cập nhật quan hệ lên knowledge graph
            
            Cập nhật quan hệ transfer
            """

            from_address = tx.get(TransactionConstant.from_address)
            to_address = tx.get(TransactionConstant.to_address)
            tx_id = tx.get(TransactionConstant.transaction_hash)
            timestamp = timestamp
            token = TokenConstant.native_token
            value = tx.get(TransactionConstant.value)

            transfer = Transfer(tx_id, timestamp, from_address, to_address, token, value)
            self.klg_database.create_transfer_relationship(transfer)
