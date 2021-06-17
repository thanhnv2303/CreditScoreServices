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

from calculate_credit_score.database.credit_score_database import Database
from calculate_credit_score.services.credit_score_service_v_0_3_0 import CreditScoreServiceV030
from executors.batch_work_executor import BatchWorkExecutor
from exporter.console_exporter import ConsoleExporter
from jobs.base_job import BaseJob
from services.eth_service import EthService

logger = logging.getLogger(__name__)
import datetime


class CalculateWalletCreditScoreJob(BaseJob):

    def __init__(
            self,
            web3,
            batch_size=24,
            max_workers=8,
            checkpoint=None,
            k_timestamp=None,
            item_exporter=ConsoleExporter(),
            database=Database(),
            list_token_filter="artifacts/token_credit_info/listToken.txt",
            token_info="artifacts/token_credit_info/infoToken.json"
    ):
        self.item_exporter = item_exporter
        self.batch_work_executor = BatchWorkExecutor(batch_size, max_workers)
        self.max_workers = max_workers
        self.paging = self.max_workers * 3
        self.web3 = web3
        self.database = database
        self.ethService = EthService(web3)
        self.end_block = self.ethService.get_latest_block()
        self.checkpoint = checkpoint
        if not self.checkpoint:
            now = datetime.datetime.now()
            self.checkpoint = str(now.year) + "-" + str(now.month) + "-" + str(now.day)

        if k_timestamp:
            self.start_block = self.ethService.get_block_at_timestamp(k_timestamp)
        else:
            self.start_block = self.end_block - 900000

        self.credit_score_services = CreditScoreServiceV030(database=database,
                                                            list_token_filter=list_token_filter,
                                                            token_info=token_info)
        self.statistics_credit = {}
        self._dict_cache = []

    def _start(self):
        self._calculate_standardized_score_info()
        self.item_exporter.open()

    def _calculate_standardized_score_info(self):
        """
        Tính các hàm chuẩn hóa cho các thuộc tính cần thiết để trước khi tính điểm tín dụng của từng v
        :rtype: object
        """

    def _export(self):
        """
        nhận vào một cấu trúc iterable

        :return:
        """
        iterable = []

        self.batch_work_executor.execute(
            iterable,
            self._export_batch,
            total_items=len(iterable)
        )

    def _export_batch(self, item):
        """
        xử lý từng item trong iterable ở hàm export
        :param item:
        :return:
        """

        pass

    def _end(self):
        self.batch_work_executor.shutdown()
        self.item_exporter.close()

    def get_cache(self):
        return self._dict_cache

    def clean_cache(self):
        self._dict_cache = []
