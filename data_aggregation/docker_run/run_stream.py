import os
import sys
from os import path

TOP_DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.insert(0, os.path.join(TOP_DIR, '../'))

from config.data_aggregation_constant import RunOnConstant
from data_aggregation.services.init_graph import init_graph_testnet, init_graph_mainnet, init_graph_ether

from data_aggregation.database.intermediary_database import IntermediaryDatabase
from services.log_services import config_log
from data_aggregation.streaming.aggregate_streamer import Klg_Streamer
from data_aggregation.streaming.aggregate_streamer_adapter import KLGLendingStreamerAdapter
from streaming.streaming_utils import configure_signals, configure_logging

import logging

from config.config import KLGLendingStreamerAdapterConfig
from services.item_exporter_creator import create_item_exporter

logger = logging.getLogger("Streaming Aggregate Data ")

if __name__ == '__main__':

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
    list_token_filter = str(KLGLendingStreamerAdapterConfig.LIST_TOKEN_FILTER)
    token_info = "artifacts/token_credit_info/infoToken.json"
    run_on = str(KLGLendingStreamerAdapterConfig.RUN_ON)
    # configure_logging(log_file)
    configure_signals()
    if log_file and log_file != "None":
        configure_logging(log_file)
    config_log()
    cur_path = os.path.dirname(os.path.realpath(__file__)) + "/../"

    # TODO: Implement fallback mechanism for provider uris instead of picking randomly
    # check provider is can connect
    output = "knowledge_graph"
    intermediary_database = IntermediaryDatabase()
    last_synced_block_file = cur_path + "data/lending_last_synced_block.txt"
    if path.exists(last_synced_block_file):
        start_block = None
    elif not start_block:
        start_block = intermediary_database.get_oldest_block_update()

    # memory_storage = MemoryStorage()
    if run_on == RunOnConstant.BSC_MAINNET:
        init_graph_mainnet()
    elif run_on == RunOnConstant.BSC_TESTNET:
        init_graph_testnet()
    elif run_on == RunOnConstant.ETH_MAINNET:
        init_graph_ether()

    streamer_adapter = KLGLendingStreamerAdapter(
        item_exporter=create_item_exporter(output),
        intermediary_database=intermediary_database,
        batch_size=batch_size,
        max_workers=max_workers,
        tokens_filter_file=tokens_filter_file
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
