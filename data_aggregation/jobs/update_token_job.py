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

from config.constant import LoggerConstant
from config.data_aggregation_constant import MemoryStorageKeyConstant
from data_aggregation.database.intermediary_database import IntermediaryDatabase
from data_aggregation.database.klg_database import KlgDatabase
from data_aggregation.services.price_service import PriceService
from database_common.memory_storage import MemoryStorage
from executors.batch_work_executor import BatchWorkExecutor
from jobs.base_job import BaseJob

logger = logging.getLogger(LoggerConstant.UpdateTokenJob)
PREVIOUS_BLOCK_NUMBER = 2880000


# Exports blocks and transactions
class UpdateTokenJob(BaseJob):

    def __init__(
            self,
            smart_contracts,
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
        self.smart_contracts = smart_contracts
        self.price_service = price_service
        current_block = self.intermediary_database.get_latest_block_update()
        self.start_filter_block = current_block - PREVIOUS_BLOCK_NUMBER

    def _start(self):
        local_storage = MemoryStorage.getInstance()
        self.update_wallet_storage: dict = local_storage.get_element(MemoryStorageKeyConstant.update_wallet)

    def _end(self):
        self.batch_work_executor.shutdown()

    def _export(self):
        self.batch_work_executor.execute(
            self.smart_contracts,
            self._export_batch,
            total_items=len(self.smart_contracts)
        )

    def _export_batch(self, smart_contracts):
        for smart_contract_address in smart_contracts:
            self._update_data_token(smart_contract_address)

    def _update_data_token(self, smart_contract_address):
        """
        update thong tin dailyFrequencyOfTransactions : Số lần giao dịch của token này trong 100 ngày gần

        :param smart_contract_address:
        :return:
        """
        number_events = self.intermediary_database.get_num_event_of_smart_contract_after_block(self.start_filter_block,
                                                                                               smart_contract_address)

        start = time.time()
        self.klg_database.update_daily_frequency_of_transactions(smart_contract_address, number_events)

        # logger.info(f"Time to update daily_frequency_of_transactions {time.time() - start}s")
