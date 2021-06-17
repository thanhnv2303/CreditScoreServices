import os
import sys

TOP_DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.insert(0, os.path.join(TOP_DIR, '../'))

import logging

from services.log_services import config_log
from config.config import CreditScoreConfig
from streaming.streaming_utils import configure_signals, configure_logging

from providers.auto import pick_random_provider_uri
from calculate_credit_score.streaming.credit_score_streamer import CreditScoreStreamer
from calculate_credit_score.streaming.credit_score_streamer_adapter import CreditScoreStreamerAdapter

config_log()
if __name__ == '__main__':
    ### get environment variables

    log_file = str(CreditScoreConfig.LOG_FILE)
    lag = int(CreditScoreConfig.LAG)
    batch_size = int(CreditScoreConfig.BATCH_SIZE)
    max_workers = int(CreditScoreConfig.MAX_WORKERS)
    start_block = int(CreditScoreConfig.START_BLOCK)
    period_seconds = int(CreditScoreConfig.PERIOD_SECONDS)
    pid_file = str(CreditScoreConfig.PID_FILE)
    block_batch_size = int(CreditScoreConfig.BLOCK_BATCH_SIZE)
    list_token_filter = str(CreditScoreConfig.LIST_TOKEN_FILTER)
    token_info = str(CreditScoreConfig.TOKEN_INFO)

    # configure_logging(log_file)
    configure_signals()
    if log_file:
        configure_logging(log_file)

    cur_path = os.path.dirname(os.path.realpath(__file__)) + "/../../"

    # TODO: Implement fallback mechanism for provider uris instead of picking randomly
    # check provider is can connect
    output = "knowledge_graph"


    last_synced_block_file = cur_path + "data/last_synced_block.txt"


    streamer_adapter = CreditScoreStreamerAdapter(
        max_workers=max_workers,
        list_token_filter=list_token_filter,
        token_info=token_info
    )
    streamer = CreditScoreStreamer(
        blockchain_streamer_adapter=streamer_adapter,
        last_synced_block_file=last_synced_block_file,
        lag=lag,
        period_seconds=period_seconds,
        block_batch_size=block_batch_size,
        retry_errors=True,
        pid_file=pid_file
    )
    streamer.stream()
