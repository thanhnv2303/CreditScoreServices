import os
import sys

TOP_DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.insert(0, os.path.join(TOP_DIR, '../'))

import logging

format = '%(asctime)s - %(name)s [%(levelname)s] - %(message)s'
logging.basicConfig(
    format=format,
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

from calculate_credit_score.streaming.credit_score_streamer import CreditScoreStreamer
from calculate_credit_score.streaming.credit_score_streamer_adapter import CreditScoreStreamerAdapter

streamer_adapter = CreditScoreStreamerAdapter()
streamer = CreditScoreStreamer(
    blockchain_streamer_adapter=streamer_adapter,
    pid_file=None
)
streamer.stream()
