import logging

from config.constant import EthKnowledgeGraphStreamerAdapterConstant
from config.data_aggregation_constant import MemoryStorageKeyConstant
from data_aggregation.database.intermediary_database import IntermediaryDatabase
from data_aggregation.database.klg_database import KlgDatabase
from data_aggregation.jobs.aggregate_native_token_transfer_job import AggregateNativeTokenTransferJob
from data_aggregation.jobs.aggregate_smart_contract_job import AggregateSmartContractJob
from data_aggregation.jobs.aggregate_wallet_job import AggregateWalletJob
from database_common.memory_storage import MemoryStorage

logger = logging.getLogger('Aggregation data')


def aggregate(start_block, end_block, max_workers, batch_size,
              item_exporter,
              event_abi_dir=EthKnowledgeGraphStreamerAdapterConstant.event_abi_dir_default,
              smart_contracts=None,
              ethTokenService=None,
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
    :param ethTokenService:
    :param intermediary_database:
    :param klg_database:
    :return:
    """

    """
    Tạo kho dữ liệu tạm để có thể lưu trữ các ví được update trong batch này
    """
    local_storage = MemoryStorage.getInstance()
    local_storage.add_element(key=MemoryStorageKeyConstant.update_wallet, value=set())
    """
    Tổng hợp thông tin theo từng transaction chuyển native token
    """
    job = AggregateNativeTokenTransferJob(start_block,
                                          end_block,
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
                             batch_size=batch_size,
                             max_workers=max_workers,
                             intermediary_database=intermediary_database,
                             klg_database=klg_database)
    pass
