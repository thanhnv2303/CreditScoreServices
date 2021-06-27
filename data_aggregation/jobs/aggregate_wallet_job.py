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
import time

from config.constant import LoggerConstant, WalletConstant
from config.data_aggregation_constant import MemoryStorageKeyConstant
from data_aggregation.database.intermediary_database import IntermediaryDatabase
from data_aggregation.database.klg_database import KlgDatabase
from data_aggregation.services.price_service import PriceService
from database_common.memory_storage import MemoryStorage
from executors.batch_work_executor import BatchWorkExecutor
from jobs.base_job import BaseJob

logger = logging.getLogger(LoggerConstant.AggregateWalletJob)


# Exports blocks and transactions
class AggregateWalletJob(BaseJob):
    def __init__(
            self,
            wallet_addresses,
            price_service=PriceService(),
            batch_size=128,
            max_workers=8,
            intermediary_database=IntermediaryDatabase(),
            klg_database=KlgDatabase()
    ):
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.batch_work_executor = BatchWorkExecutor(batch_size, max_workers)
        self.intermediary_database = intermediary_database
        self.klg_database = klg_database
        self.wallet_addresses = wallet_addresses
        self.price_service = price_service

    def _start(self):
        local_storage = MemoryStorage.getInstance()
        self.update_wallet_storage: dict = local_storage.get_element(MemoryStorageKeyConstant.update_wallet)

    def _end(self):
        self.batch_work_executor.shutdown()

    def _export(self):
        self.batch_work_executor.execute(
            self.wallet_addresses,
            self._export_batch,
            total_items=len(self.wallet_addresses)
        )
        # self._export_batch(self.wallet_addresses)

    def _export_batch(self, wallet_addresses):
        for wallet_address in wallet_addresses:
            self._export_data_wallet(wallet_address)

    def _export_data_wallet(self, wallet_address):
        try:
            # wallet = self.intermediary_database.get_wallet(wallet_address)
            start1 = time.time()
            wallet = self.update_wallet_storage.get(wallet_address)
            logger.info(f"Time to get wallet at storage is {time.time() - start1}")
            """
            update thông tin ví lên knowledge graph 
            """
            wallet_token = wallet.get(WalletConstant.balance)
            ### update wallet_token vao truong Token cua vi trong klg
            if wallet_token:
                start = time.time()
                total_balance = self.price_service.get_total_value(wallet_token)
                self.klg_database.update_wallet_token(wallet_address, wallet_token, total_balance)
                logger.info(f"Time to update balance wallet {time.time() - start}")
            wallet_token_deposit = wallet.get(WalletConstant.supply)
            wallet_token_borrow = wallet.get(WalletConstant.borrow)
            if wallet_token_deposit and wallet_token_borrow:
                start = time.time()
                deposit_balance = self.price_service.get_total_value(wallet_token_deposit)
                borrow_balance = self.price_service.get_total_value(wallet_token_borrow)

                self.klg_database.update_wallet_token_deposit_and_borrow(
                    wallet_address,
                    wallet_token_deposit,
                    wallet_token_borrow,
                    deposit_balance,
                    borrow_balance
                )

                logger.info(f"Time to update lending info wallet {time.time() - start}")

            """
            cập nhật thông tin về ngày xuất hiện giao dịch đầu tiên trên hệ thống
            """
            start = time.time()
            created_at = self.klg_database.get_wallet_created_at(wallet_address)
            if not created_at:
                try:
                    create_at = self.intermediary_database.get_first_create_wallet(wallet_address.lower())
                except:
                    create_at = self.intermediary_database.block_number_to_time_stamp(wallet.get("block_number"))
                self.klg_database.update_wallet_created_at(wallet_address, create_at)
            logger.info(f"Total time to update create at of wallet {time.time() - start}")

            logger.info(f"Total time to update a wallet is {time.time() - start1}")
            ###
        except Exception as e:
            logger.error(e)
