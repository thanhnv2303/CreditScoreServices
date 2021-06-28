import logging
import os
import sys

TOP_DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.insert(0, os.path.join(TOP_DIR, '../'))

from services.log_services import config_log
import time

from calculate_credit_score.test.get_statistics_info import get_statistics
from config.config import CreditScoreConfig

logger = logging.getLogger("Run get statistic frequently")

config_log()

logger.info("Start get statistic info ...")
while True:
    get_statistics()
    period_seconds = int(CreditScoreConfig.PERIOD_SECONDS)
    logger.info(f"Get credit score statistic info done sleep for {period_seconds}s")
    time.sleep(period_seconds)
