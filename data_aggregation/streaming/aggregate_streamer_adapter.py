import logging
import os
from time import time

from config.constant import EthKnowledgeGraphStreamerAdapterConstant
from data_aggregation.database.intermediary_database import IntermediaryDatabase
from data_aggregation.database.klg_database import KlgDatabase
from data_aggregation.jobs.aggregation_data import aggregate
from data_aggregation.services.price_service import PriceService
from exporter.console_item_exporter import ConsoleItemExporter
from services.eth_item_id_calculator import EthItemIdCalculator
from services.eth_item_timestamp_calculator import EthItemTimestampCalculator

logger = logging.getLogger('export_lending_graph')


class KLGLendingStreamerAdapter:
    def __init__(
            self,
            item_exporter=ConsoleItemExporter(),
            intermediary_database=IntermediaryDatabase(),
            klg_database=KlgDatabase(),
            batch_size=100,
            max_workers=5,
            tokens_filter_file="artifacts/token_filter",
            v_tokens_filter_file="artifacts/vToken_filter",
            list_token_filter="artifacts/token_credit_info/listToken.txt",
            token_info="artifacts/token_credit_info/infoToken.json"
    ):

        self.item_exporter = item_exporter
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.item_id_calculator = EthItemIdCalculator()
        self.item_timestamp_calculator = EthItemTimestampCalculator()
        self.intermediary_database = intermediary_database
        self.klg_database = klg_database
        cur_path = os.path.dirname(os.path.realpath(__file__)) + "/../../"
        self.tokens_filter_file = cur_path + tokens_filter_file
        self.v_tokens_filter_file = cur_path + v_tokens_filter_file

        self.credit_score_service = PriceService(intermediary_database, list_token_filter, token_info)
        self.list_token_filter = list_token_filter
        self.token_info = token_info

    def open(self):
        self.item_exporter.open()

    def get_current_block_number(self):
        """
        xác định điểm đồng bộ hiện tại trong cơ sở dữ liệu trung gian
        :return:
        """
        try:
            return self.intermediary_database.get_latest_block_update()
        except Exception as e:
            logger.error(e)
            return 0

    def export_all(self, start_block, end_block):
        start_time = time()
        self.aggregate_data(start_block, end_block)
        end_time = time()
        time_diff = round(end_time - start_time, 5)
        logger.info('Exporting blocks {block_range} took {time_diff} seconds'.format(
            block_range=(end_block - start_block + 1),
            time_diff=time_diff,
        ))

    def close(self):
        self.item_exporter.close()

    def aggregate_data(self, start_block, end_block):
        """
        Bắt đầu tổng hợp dữ liệu
        :return:
        """

        with open(self.tokens_filter_file, "r") as file:
            tokens_list = file.read().splitlines()
            smart_contracts = []
            for token in tokens_list:
                smart_contracts.append(token.lower())

        aggregate(start_block, end_block, self.max_workers, self.batch_size,
                  event_abi_dir=EthKnowledgeGraphStreamerAdapterConstant.event_abi_dir_default,
                  smart_contracts=smart_contracts,
                  credit_score_service=self.credit_score_service,
                  intermediary_database=self.intermediary_database,
                  klg_database=self.klg_database
                  )


def get_nearest_less_key(dict_, search_key):
    try:
        result = max(key for key in map(int, dict_.keys()) if key <= search_key)
        return result
    except:
        return


def get_nearest_key(dict_, search_key):
    try:
        result = min(dict_.keys(), key=lambda key: abs(key - search_key))
        return result
    except:
        return
