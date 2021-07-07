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
from data_aggregation.services.time_service import round_timestamp_to_date
from database_common.memory_storage import MemoryStorage
from executors.batch_work_executor import BatchWorkExecutor
from jobs.base_job import BaseJob
from utils.to_number import to_float, to_int

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
            tx_id = tx.get(TransactionConstant.transaction_hash)
            timestamp_day = timestamp
            daily_timestamp = round_timestamp_to_date(timestamp)
            if not related_wallets:
                return
            for wallet in related_wallets:
                """
                 thêm thông tin địa chỉ ví vào kho để update sau cho các thông tin không cần lịch sử.
                """
                # logger.info("wallet--------------------")
                # logger.info(wallet)
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
                dailyFrequencyOfTransactions  - trong credit score(non standardized): số giao dịch của wallet trong k ngày, mảng gồm 100 ngày
                """

                dict_timestamp = self.klg_database.get_daily_daily_frequency_of_transaction(wallet_address)
                current_value_timestamp = to_int(dict_timestamp.get(daily_timestamp))
                dict_timestamp[daily_timestamp] = current_value_timestamp + 1
                self.klg_database.update_daily_frequency_of_transaction(wallet_address, dict_timestamp)
            """
            dailyTransactionAmounts: Tổng giá trị giao dịch của wallet trong 100 ngày, mảng gồm 100 ngày - lưu ý chỉ tính giao dịch chuyển tiền tới tài khoản này
            """

            token = TokenConstant.native_token
            to_address = tx.get(TransactionConstant.to_address)
            value = tx.get(TransactionConstant.value)
            daily_transaction_amount = self.klg_database.get_daily_transaction_amount_100(to_address)
            current_value_timestamp = to_float(daily_transaction_amount.get(daily_timestamp))
            value_usd = self.price_service.token_amount_to_usd(token, value)
            daily_transaction_amount[daily_timestamp] = current_value_timestamp + value_usd
            self.klg_database.update_daily_transaction_amount_100(to_address, daily_transaction_amount)

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

            value_usd = self.price_service.token_amount_to_usd(token, value)
            transfer = Transfer(tx_id, timestamp, from_address, to_address, token, value, value_usd)
            self.klg_database.create_transfer_relationship(transfer)
