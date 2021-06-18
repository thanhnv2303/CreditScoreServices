import logging
from datetime import datetime
from time import time

from config.constant import EthKnowledgeGraphStreamerAdapterConstant
from config.data_aggregation_constant import MemoryStorageKeyConstant
from data_aggregation.database.intermediary_database import IntermediaryDatabase
from data_aggregation.database.klg_database import KlgDatabase
from data_aggregation.jobs.aggregate_native_token_transfer_job import AggregateNativeTokenTransferJob
from data_aggregation.jobs.aggregate_smart_contract_job import AggregateSmartContractJob
from data_aggregation.jobs.aggregate_wallet_job import AggregateWalletJob
from data_aggregation.services.credit_score_service_v_0_3_0 import CreditScoreServiceV030
from database_common.memory_storage import MemoryStorage

logger = logging.getLogger('Aggregation data')


def aggregate(start_block, end_block, max_workers, batch_size,
              event_abi_dir=EthKnowledgeGraphStreamerAdapterConstant.event_abi_dir_default,
              smart_contracts=None,
              credit_score_service=CreditScoreServiceV030(),
              intermediary_database=IntermediaryDatabase(),
              klg_database=KlgDatabase()
              ):
    """
    Tiến hành tổng hợp thông tin của ví theo từng block
    :param start_block:
    :param end_block:
    :param max_workers:
    :param batch_size:
    :param item_exporter:
    :param event_abi_dir:
    :param smart_contracts:
    :param credit_score_service:
    :param intermediary_database:
    :param klg_database:
    :return:
    """

    """
    Cập nhật giá của các đồng vào một thời điểm cố định trong ngày
    """
    now = datetime.now()
    if now.hour == 3 and now.minute < 5:
        credit_score_service.update_token_info()
    """
    Tạo kho dữ liệu tạm để có thể lưu trữ các ví được update trong batch này
    """
    start_time = time()
    local_storage = MemoryStorage.getInstance()
    local_storage.add_element(key=MemoryStorageKeyConstant.update_wallet, value=set())
    """
    Tổng hợp thông tin theo từng transaction chuyển native token
    """
    job = AggregateNativeTokenTransferJob(start_block,
                                          end_block,
                                          credit_score_service=credit_score_service,
                                          batch_size=batch_size,
                                          max_workers=max_workers,
                                          intermediary_database=intermediary_database,
                                          klg_database=klg_database)

    job.run()
    """
    Tổng hợp thông tin theo từng event của các smart contract
    """

    job = AggregateSmartContractJob(start_block,
                                    end_block,
                                    smart_contracts=smart_contracts,
                                    credit_score_service=credit_score_service,
                                    batch_size=batch_size,
                                    max_workers=max_workers,
                                    intermediary_database=intermediary_database,
                                    klg_database=klg_database)

    job.run()
    """
    Tổng hợp thông tin của các ví có thay đổi dữ liệu đến thời điểm hiện tại
    """
    wallets_updated = local_storage.get_element(key=MemoryStorageKeyConstant.update_wallet)
    job = AggregateWalletJob(wallets_updated,
                             credit_score_service=credit_score_service,
                             batch_size=batch_size,
                             max_workers=max_workers,
                             intermediary_database=intermediary_database,
                             klg_database=klg_database)
    job.run()

    end_time = time()
    time_diff = round(end_time - start_time, 5)
    block_range = '{start_block}-{end_block}'.format(
        start_block=start_block,
        end_block=end_block,
    )
    logger.info('Exporting blocks {block_range} took {time_diff} seconds'.format(
        block_range=block_range,
        time_diff=time_diff,
    ))
