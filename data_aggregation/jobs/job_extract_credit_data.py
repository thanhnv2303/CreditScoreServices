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
import logging

from data_aggregation.database.intermediary_database import IntermediaryDatabase
from database_common.memory_storage import MemoryStorage
from executors.batch_work_executor import BatchWorkExecutor
from exporter.console_exporter import ConsoleExporter
from jobs.base_job import BaseJob
from services.eth_service import EthService

logger = logging.getLogger(__name__)


class ExtractCreditDataJob(BaseJob):
    def _start(self):
        self.item_exporter.open()

    def __init__(
            self,
            web3,
            batch_size=128,
            max_workers=8,
            checkpoint=None,
            k_timestamp=None,
            item_exporter=ConsoleExporter(),
            database=IntermediaryDatabase(),
    ):
        self.item_exporter = item_exporter
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.batch_work_executor = BatchWorkExecutor(batch_size, max_workers)
        self.paging = self.max_workers * 4
        self.database = database
        self.ethService = EthService(web3)
        self.end_block = self.ethService.get_latest_block()
        self.checkpoint = checkpoint
        if k_timestamp:
            self.start_block = self.ethService.get_block_at_timestamp(k_timestamp)
        else:
            self.start_block = self.end_block - 900000

        self.extractors = []
        self._add_extractor()

        self.local_storage = MemoryStorage.getInstance()

    def _add_extractor(self):
        pass

    def _export(self):

        wallets = []
        if len(wallets) == 0:
            return
        self.batch_work_executor.execute(
            wallets,
            self._export_batch,
        )

    def _export_batch(self, wallet):
        for extractor in self.extractors:
            extractor.extract(wallet)

        pass

    def _end(self):
        ### save statistic value to database_common
        self.batch_work_executor.shutdown()
        self.item_exporter.close()

    def get_cache(self):
        return self._dict_cache

    def clean_cache(self):
        self._dict_cache = []
