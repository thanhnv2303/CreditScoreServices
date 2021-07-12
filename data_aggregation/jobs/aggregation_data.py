import logging
from time import time

from config.data_aggregation_constant import MemoryStorageKeyConstant
from config.performance_constant import PerformanceConstant
from data_aggregation.database.intermediary_database import IntermediaryDatabase
from data_aggregation.database.klg_database import KlgDatabase
from data_aggregation.jobs.aggregate_native_token_transfer_job import AggregateNativeTokenTransferJob
from data_aggregation.jobs.aggregate_smart_contract_job import AggregateSmartContractJob
from data_aggregation.jobs.aggregate_wallet_job import AggregateWalletJob
from data_aggregation.jobs.update_token_job import UpdateTokenJob
from data_aggregation.services.price_service import PriceService
from data_aggregation.services.time_service import round_timestamp_to_date
from database_common.memory_storage import MemoryStorage
from database_common.memory_storage_test_performance import MemoryStoragePerformance

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

    ### set up for carculate performance
    performance_storage = MemoryStoragePerformance.getInstance()
    performance_constant_keys = PerformanceConstant().get_all_attr()
    for key in performance_constant_keys:
        performance_storage.set(key, 0)

    if not checkpoint or checkpoint != timestamp_day:
        start = time()
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
        performance_storage.accumulate_to_key(PerformanceConstant.UpdateTokenJob, time() - start)
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
    performance_storage.accumulate_to_key(PerformanceConstant.AggregateNativeTokenTransferJob, time() - start1)
    # logger.info(f"time to aggreegate token transfer native {time() - start1}")
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
    performance_storage.accumulate_to_key(PerformanceConstant.AggregateSmartContractJob, time() - start2)
    # logger.info(f"AggregateSmartContractJob {time() - start2} ")
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
    performance_storage.accumulate_to_key(PerformanceConstant.AggregateWalletJob, time() - start3)
    # logger.info(f"Time to AggregateWalletJob {time() - start3}")

    read_mongo_time = performance_storage.get(PerformanceConstant.block_number_to_time_stamp) + \
                      performance_storage.get(PerformanceConstant.get_latest_block_update) + \
                      performance_storage.get(PerformanceConstant.get_oldest_block_update) + \
                      performance_storage.get(PerformanceConstant.get_first_create_wallet) + \
                      performance_storage.get(PerformanceConstant.get_transfer_native_token_tx_in_block) + \
                      performance_storage.get(PerformanceConstant.get_events_at_of_smart_contract) + \
                      performance_storage.get(PerformanceConstant.get_wallet) + \
                      performance_storage.get(PerformanceConstant.get_token)

    read_neo4j_time = performance_storage.get(PerformanceConstant.get_token_prices) + \
                      performance_storage.get(PerformanceConstant.get_wallet_created_at) + \
                      performance_storage.get(PerformanceConstant.get_balance_100) + \
                      performance_storage.get(PerformanceConstant.get_daily_transaction_amount_100) + \
                      performance_storage.get(PerformanceConstant.get_daily_daily_frequency_of_transaction) + \
                      performance_storage.get(PerformanceConstant.get_num_of_liquidation_100) + \
                      performance_storage.get(PerformanceConstant.get_total_amount_liquidation_100) + \
                      performance_storage.get(PerformanceConstant.get_deposit_100) + \
                      performance_storage.get(PerformanceConstant.get_borrow_100) + \
                      performance_storage.get(PerformanceConstant.get_info_relationship)

    write_neo4j_time = performance_storage.get(PerformanceConstant.update_wallet_token) + \
                       performance_storage.get(PerformanceConstant.update_wallet_token_deposit_and_borrow) + \
                       performance_storage.get(PerformanceConstant.update_wallet_created_at) + \
                       performance_storage.get(PerformanceConstant.update_balance_100) + \
                       performance_storage.get(PerformanceConstant.update_daily_transaction_amount_100) + \
                       performance_storage.get(PerformanceConstant.update_daily_frequency_of_transaction) + \
                       performance_storage.get(PerformanceConstant.update_num_of_liquidation_100) + \
                       performance_storage.get(PerformanceConstant.update_total_amount_liquidation_100) + \
                       performance_storage.get(PerformanceConstant.update_deposit_100) + \
                       performance_storage.get(PerformanceConstant.update_borrow_100) + \
                       performance_storage.get(PerformanceConstant.update_daily_frequency_of_transactions) + \
                       performance_storage.get(PerformanceConstant.create_transfer_relationship) + \
                       performance_storage.get(PerformanceConstant.create_deposit_relationship) + \
                       performance_storage.get(PerformanceConstant.create_borrow_relationship) + \
                       performance_storage.get(PerformanceConstant.create_repay_relationship) + \
                       performance_storage.get(PerformanceConstant.create_withdraw_relationship) + \
                       performance_storage.get(PerformanceConstant.create_liquidate_relationship)

    performance_storage.set(PerformanceConstant.read_mongo_time, read_mongo_time)
    performance_storage.set(PerformanceConstant.read_neo4j_time, read_neo4j_time)
    performance_storage.set(PerformanceConstant.write_neo4j_time, write_neo4j_time)

    end_time = time()
    time_diff = round(end_time - start_time, 5)
    block_range = '{start_block}-{end_block}'.format(
        start_block=start_block,
        end_block=end_block,
    )

    for key in performance_constant_keys:
        logger.info(f"Export blocks {block_range} for {key} take {performance_storage.get(key)}")

    logger.info('Exporting blocks {block_range} took {time_diff} seconds'.format(
        block_range=block_range,
        time_diff=time_diff,
    ))
