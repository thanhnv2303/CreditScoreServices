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

from config.constant import LoggerConstant
from config.data_aggregation_constant import MemoryStorageKeyConstant
from data_aggregation.database.intermediary_database import IntermediaryDatabase
from data_aggregation.database.klg_database import KlgDatabase
from data_aggregation.services.credit_score_service_v_0_3_0 import CreditScoreServiceV030
from database_common.memory_storage import MemoryStorage
from executors.batch_work_executor import BatchWorkExecutor
from jobs.base_job import BaseJob

logger = logging.getLogger(LoggerConstant.ExportBlocksJob)


# Exports blocks and transactions
class AggregateWalletJob(BaseJob):
    def __init__(
            self,
            wallet_addresses,
            credit_score_service=CreditScoreServiceV030(),
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
        self.credit_score_service = credit_score_service

    def _start(self):
        local_storage = MemoryStorage.getInstance()
        self.update_wallet_storage: set = local_storage.get_element(MemoryStorageKeyConstant.update_wallet)

    def _export(self):
        self.batch_work_executor.execute(
            self.wallet_addresses,
            self._export_batch,
            total_items=len(self.wallet_addresses)
        )

    def _export_batch(self, wallet_address):
        self._export_data_wallet(wallet_address)

    def _export_data_wallet(self, wallet_address):
        wallet = self.intermediary_database.get_wallet(wallet_address)

        """
        update thông tin ví lên knowledge graph 
        """
