import logging
import os
from time import time

from data_aggregation.database.aggregate_database import Database
from exporter.console_item_exporter import ConsoleItemExporter
from services.eth_item_id_calculator import EthItemIdCalculator
from services.eth_item_timestamp_calculator import EthItemTimestampCalculator

logger = logging.getLogger('export_lending_graph')


class KLGLendingStreamerAdapter:
    def __init__(
            self,
            item_exporter=ConsoleItemExporter(),
            database=Database(),
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
        self.database = database
        cur_path = os.path.dirname(os.path.realpath(__file__)) + "/../../"
        self.tokens_filter_file = cur_path + tokens_filter_file
        self.v_tokens_filter_file = cur_path + v_tokens_filter_file

        self.list_token_filter = list_token_filter
        self.token_info = token_info

    def open(self):
        self.item_exporter.open()

    def get_current_block_number(self):
        """
        xác định điểm đồng bộ hiện tại
        :return:
        """
        try:
            # latest_block = self.database.mongo_blocks.find_one(sort=[("number", -1)])  # for MAX
            # return latest_block.get("number") - 16
            return 100000
        except Exception as e:
            logger.error(e)
            return 0

    def export_all(self, start_block, end_block):
        start_time = time()
        self.aggregate_data()
        end_time = time()
        time_diff = round(end_time - start_time, 5)
        logger.info('Exporting blocks {block_range} took {time_diff} seconds'.format(
            block_range=(end_block - start_block + 1),
            time_diff=time_diff,
        ))

    def close(self):
        self.item_exporter.close()

    def aggregate_data(self, ):
        """
        Bắt đầu tổng hợp dữ liệu
        :return:
        """
        logger.info("Aggregate data ... ")


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
