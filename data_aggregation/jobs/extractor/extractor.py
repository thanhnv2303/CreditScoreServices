import logging
import threading
import time

from data_aggregation.database.intermediary_database import IntermediaryDatabase
from database_common.memory_storage import MemoryStorage
from services.credit_score_service_v_0_2_0 import CreditScoreServiceV020

logger = logging.getLogger("--Extractor--")


class Extractor:
    def __init__(self, start_block, end_block, checkpoint, database=IntermediaryDatabase()):
        self.database = database
        self.start_block = start_block
        self.end_block = end_block
        self.credit_score_service = CreditScoreServiceV020(database)
        self.checkpoint = checkpoint
        self.local_storage = MemoryStorage.getInstance()
        self.statistic_credit = {}
        self.lock = threading.Lock()
        self.amount = 0

    def extract(self, data):
        pass

    def save_statistic_credit(self):
        if self.statistic_credit:
            self.statistic_credit["checkpoint"] = self.checkpoint
            self.database.update_statistic_credit(statistic_credit=self.statistic_credit)

    def add_to_statistic_list(self, list_name, address, value):

        list_value = self.local_storage.get_element(list_name)
        if not list_value:
            self.local_storage.add_element(list_name, [value])
        else:
            list_value.append(value)
        self.amount += 1
