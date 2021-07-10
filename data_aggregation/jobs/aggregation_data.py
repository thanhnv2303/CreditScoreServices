import logging
from time import time

from config.data_aggregation_constant import MemoryStorageKeyConstant
from data_aggregation.database.intermediary_database import IntermediaryDatabase
from data_aggregation.database.klg_database import KlgDatabase
from data_aggregation.jobs.aggregate_native_token_transfer_job import AggregateNativeTokenTransferJob
from data_aggregation.jobs.aggregate_smart_contract_job import AggregateSmartContractJob
from data_aggregation.jobs.aggregate_wallet_job import AggregateWalletJob
from data_aggregation.jobs.update_token_job import UpdateTokenJob
from data_aggregation.services.price_service import PriceService
from data_aggregation.services.time_service import round_timestamp_to_date
from database_common.memory_storage import MemoryStorage

logger = logging.getLogger('Aggregation data')


def aggregate(start_block, end_block, max_workers, batch_size,
              smart_contracts=None,
              credit_score_service=PriceService(),
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
    :param smart_contracts:
    :param credit_score_service:
    :param intermediary_database:
    :param klg_database:
    :return:
    """

    """
        Tạo kho dữ liệu tạm để có thể lưu trữ các ví được update trong batch này
        """
    start_time = time()
    local_storage = MemoryStorage.getInstance()
    local_storage.set_element(key=MemoryStorageKeyConstant.update_wallet, value=dict())

    checkpoint = local_storage.get_element(MemoryStorageKeyConstant.checkpoint)
    timestamp = round(start_time)
    timestamp_day = round_timestamp_to_date(timestamp)
    if not checkpoint or checkpoint != timestamp_day:
        """
        Cập nhật giá của các đồng vào một thời điểm cố định trong ngày
        """
        logger.info(
            """
            Cập nhật giá của các đồng vào một thời điểm cố định trong ngày
            """
        )
        credit_score_service.update_token_market_info()
        logger.info(
            """
            Update thông tin Số lần giao dịch của token này trong 100 ngày gần
            """
        )
        """
        Update thông tin Số lần giao dịch của token này trong 100 ngày gần
        """
        job = UpdateTokenJob(smart_contracts=smart_contracts,
                             price_service=credit_score_service,
                             batch_size=batch_size,
                             max_workers=max_workers,
                             intermediary_database=intermediary_database,
                             klg_database=klg_database)
        job.run()

        local_storage.set_element(MemoryStorageKeyConstant.checkpoint, timestamp_day)
    """
    Tổng hợp thông tin theo từng transaction chuyển native token
    """
    logger.info("""
    Tổng hợp thông tin theo từng transaction chuyển native token
    """)
    start1 = time()
    job = AggregateNativeTokenTransferJob(start_block,
                                          end_block,
                                          price_service=credit_score_service,
                                          batch_size=batch_size,
                                          max_workers=max_workers,
                                          intermediary_database=intermediary_database,
                                          klg_database=klg_database)

    job.run()

    logger.info(f"time to aggreegate token transfer native {time() - start1}")
    """
    Tổng hợp thông tin theo từng event của các smart contract
    """
    logger.info("""
    Tổng hợp thông tin theo từng event của các smart contract
    """)

    start2 = time()
    job = AggregateSmartContractJob(start_block,
                                    end_block,
                                    smart_contracts=smart_contracts,
                                    credit_score_service=credit_score_service,
                                    batch_size=batch_size,
                                    max_workers=max_workers,
                                    intermediary_database=intermediary_database,
                                    klg_database=klg_database)

    job.run()

    logger.info(f"AggregateSmartContractJob {time() - start2} ")
    """
    Tổng hợp thông tin của các ví có thay đổi dữ liệu đến thời điểm hiện tại
    """
    logger.info("""
    Tổng hợp thông tin của các ví có thay đổi dữ liệu đến thời điểm hiện tại
    """)
    start3 = time()
    wallets_updated = local_storage.get_element(key=MemoryStorageKeyConstant.update_wallet)
    job = AggregateWalletJob(wallets_updated,
                             price_service=credit_score_service,
                             batch_size=batch_size,
                             max_workers=max_workers,
                             intermediary_database=intermediary_database,
                             klg_database=klg_database)
    job.run()

    logger.info(f"Time to AggregateWalletJob {time() - start3}")

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
