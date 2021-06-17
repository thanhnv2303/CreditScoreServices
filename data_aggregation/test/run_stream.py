import os
import sys
from os import path

TOP_DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.insert(0, os.path.join(TOP_DIR, '../'))

from services.log_services import config_log
from data_aggregation.streaming.aggregate_streamer import Klg_Streamer
from data_aggregation.streaming.aggregate_streamer_adapter import KLGLendingStreamerAdapter
from streaming.streaming_utils import configure_signals, configure_logging

import logging

from config.config import KLGLendingStreamerAdapterConfig
from services.item_exporter_creator import create_item_exporter

logger = logging.getLogger("Streaming Aggregate Data ")
config_log()
if __name__ == '__main__':
    logger.info("Start streaming ...")
    log_file = str(KLGLendingStreamerAdapterConfig.LOG_FILE)
    lag = int(KLGLendingStreamerAdapterConfig.LAG)
    batch_size = int(KLGLendingStreamerAdapterConfig.BATCH_SIZE)
    max_workers = int(KLGLendingStreamerAdapterConfig.MAX_WORKERS)
    start_block = int(KLGLendingStreamerAdapterConfig.START_BLOCK)
    period_seconds = int(KLGLendingStreamerAdapterConfig.PERIOD_SECONDS)
    pid_file = str(KLGLendingStreamerAdapterConfig.PID_FILE)
    block_batch_size = int(KLGLendingStreamerAdapterConfig.BLOCK_BATCH_SIZE)
    tokens_filter_file = str(KLGLendingStreamerAdapterConfig.TOKENS_FILTER_FILE)
    v_tokens_filter_file = str(KLGLendingStreamerAdapterConfig.V_TOKENS_FILTER_FILE)
    list_token_filter = "artifacts/token_credit_info/listToken.txt"
    token_info = "artifacts/token_credit_info/infoToken.json"
    # configure_logging(log_file)
    configure_signals()
    logger.info(log_file)
    if log_file and log_file != "None":
        configure_logging(log_file)

    cur_path = os.path.dirname(os.path.realpath(__file__)) + "/../"

    # TODO: Implement fallback mechanism for provider uris instead of picking randomly

    # check provider is can connect
    output = "knowledge_graph"

    last_synced_block_file = cur_path + "data/lending_last_synced_block.txt"
    if path.exists(last_synced_block_file):
        start_block = None
    elif not start_block:
        start_block = 0

    # memory_storage = MemoryStorage()
    streamer_adapter = KLGLendingStreamerAdapter(
        item_exporter=create_item_exporter(output),
        batch_size=batch_size,
        max_workers=max_workers,
        tokens_filter_file=tokens_filter_file,
        v_tokens_filter_file=v_tokens_filter_file,
        list_token_filter=list_token_filter,
        token_info=token_info
    )
    streamer = Klg_Streamer(
        blockchain_streamer_adapter=streamer_adapter,
        last_synced_block_file=last_synced_block_file,
        lag=lag,
        start_block=start_block,
        period_seconds=period_seconds,
        block_batch_size=block_batch_size,
        pid_file=pid_file
    )
    streamer.stream()
