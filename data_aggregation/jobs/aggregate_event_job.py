# MIT License
#
# Copyright (c) 2018 Evgeny Medvedev, evge.medvedev@gmail.com
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
# import asyncio
import logging
import time

from config.constant import LoggerConstant, TransactionConstant, WalletConstant, EventConstant
from config.data_aggregation_constant import MemoryStorageKeyConstant, EventTypeAggregateConstant, DepositEventConstant, \
    MintEventConstant, BorrowEventVTokenConstant, BorrowEventPoolConstant, RedeemEventConstant, WithdrawEventConstant, \
    RepayEventConstant, RepayBorrowEventConstant, LiquidateBorrowEventConstant, LiquidationCallEventConstant
from data_aggregation.database.intermediary_database import IntermediaryDatabase
from data_aggregation.database.klg_database import KlgDatabase
from data_aggregation.database.relationships_model import Transfer, Deposit, Borrow, Withdraw, Repay, Liquidate
from data_aggregation.services.price_service import PriceService
from data_aggregation.services.time_service import round_timestamp_to_date
from database_common.memory_storage import MemoryStorage
from executors.batch_work_executor import BatchWorkExecutor
from jobs.base_job import BaseJob
from services.zip_service import dict_to_two_list
from utils.to_number import to_float, to_int

logger = logging.getLogger(LoggerConstant.AggregateEventJob)


# Exports blocks and transactions
class AggregateEventJob(BaseJob):
    def __init__(
            self,
            start_block,
            end_block,
            smart_contract,
            price_service=PriceService(),
            batch_size=128,
            max_workers=8,
            intermediary_database=IntermediaryDatabase(),
            klg_database=KlgDatabase()
    ):
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.batch_work_executor = BatchWorkExecutor(batch_size, max_workers)
        self.intermediary_database = intermediary_database
        self.klg_database = klg_database
        self.end_block = end_block
        self.start_block = start_block
        self.smart_contract = smart_contract
        self.price_service = price_service

        self.map_handler = {
            EventTypeAggregateConstant.Transfer: self._transfer_handler,
            EventTypeAggregateConstant.Borrow: self._borrow_handler,
            EventTypeAggregateConstant.Deposit: self._deposit_handler,
            EventTypeAggregateConstant.LiquidateBorrow: self._liquidate_borrow_handler,
            EventTypeAggregateConstant.LiquidationCall: self.liquidate_call_handler,
            EventTypeAggregateConstant.Mint: self._mint_handler,
            EventTypeAggregateConstant.Redeem: self._redeem_handler,
            EventTypeAggregateConstant.Repay: self._repay_handler,
            EventTypeAggregateConstant.RepayBorrow: self._repay_borrow_handler,
            EventTypeAggregateConstant.Withdraw: self._withdraw_handler,
        }

    def _start(self):
        local_storage = MemoryStorage.getInstance()
        self.update_wallet_storage: dict = local_storage.get_element(MemoryStorageKeyConstant.update_wallet)

    def _end(self):
        self.batch_work_executor.shutdown()

    def _export(self):
        self.batch_work_executor.execute(
            range(self.start_block, self.end_block + 1),
            self._export_batch,
            total_items=self.end_block - self.start_block + 1
        )

    def _export_batch(self, block_number_batch):
        for block_number in block_number_batch:
            self._export_data_events(block_number)

    def _export_data_events(self, block):
        start = time.time()
        events = self.intermediary_database.get_events_at_of_smart_contract(block,
                                                                            smart_contract_address=self.smart_contract)
        # logger.info(f" Time to gets events from db {time.time() - start}")
        start = time.time()
        events = list(events)
        # logger.info(f"Time go get {len(events)} events to list {time.time() - start} ")
        for event in events:
            try:
                # logger.info("eventttttttttttttttttttttttttttttttttttttt")
                # logger.info(event)
                event_type = event.get(EventConstant.type)

                timestamp = event.get(TransactionConstant.block_timestamp)
                if not timestamp:
                    block_number = event.get(TransactionConstant.block_number)
                    timestamp = self.intermediary_database.block_number_to_time_stamp(block_number)

                daily_timestamp = round_timestamp_to_date(timestamp)
                timestamp_day = timestamp
                related_wallets = event.get(TransactionConstant.related_wallets)
                if not related_wallets:
                    return
                for wallet in related_wallets:
                    """
                     thêm thông tin địa chỉ ví vào kho để update sau cho các thông tin không cần lịch sử.
                    """
                    wallet_address = wallet.get(WalletConstant.address)
                    wallet_in_storage = self.update_wallet_storage.get(wallet_address)
                    if wallet_in_storage and wallet_in_storage.get(TransactionConstant.block_number) > wallet.get(
                            TransactionConstant.block_number):
                        pass
                    else:
                        self.update_wallet_storage[wallet_address] = wallet

                    """
                    thêm dữ liệu biến động số dư vào trong node wallet trong knowledge graph
                    """
                    balance = wallet.get(WalletConstant.balance)
                    balance_value = self.price_service.get_total_value(balance)
                    balance_100 = self.klg_database.get_balance_100(wallet_address)
                    balance_100[timestamp_day] = balance_value

                    self.klg_database.update_balance_100(wallet_address, balance_100)

                    """
                    thêm dữ liệu biến động deposit va borrow vào trong node wallet trong knowledge graph
                    """
                    if event_type != EventTypeAggregateConstant.Transfer:
                        logger.info("event lending")
                        logger.info(event)
                        deposit = wallet.get(WalletConstant.supply)
                        deposit_value = self.price_service.get_total_value(deposit)
                        deposit_100 = self.klg_database.get_deposit_100(wallet_address)
                        deposit_100[timestamp_day] = deposit_value

                        self.klg_database.update_deposit_100(wallet_address, deposit_100)

                        borrow = wallet.get(WalletConstant.borrow)
                        borrow_value = self.price_service.get_total_value(borrow)
                        borrow_100 = self.klg_database.get_borrow_100(wallet_address)
                        borrow_100[timestamp_day] = borrow_value

                        self.klg_database.update_borrow_100(wallet_address, borrow_100)

                    """
                    dailyFrequencyOfTransactions  - trong credit score(non standardized): số giao dịch của wallet trong k ngày, mảng gồm 100 ngày
                    """

                    dict_timestamp = self.klg_database.get_daily_daily_frequency_of_transaction(wallet_address)
                    current_value_timestamp = to_int(dict_timestamp.get(daily_timestamp))
                    dict_timestamp[daily_timestamp] = current_value_timestamp + 1
                    self.klg_database.update_daily_frequency_of_transaction(wallet_address, dict_timestamp)
                """
                xử lý tạo relationship giữa các node theo từng loại sự kiện 
                """
                handler = self._handler_event(event_type)
                if handler:
                    handler(event, timestamp)
                else:
                    logger.info(f" event {event_type} has not been supported to update to knowledge graph")
            except Exception as e:
                logger.error(e)

    def _handler_event(self, event_type):
        """

        :param event_type: 
        :return: 
        """

        return self.map_handler.get(event_type)

    def _transfer_handler(self, event, timestamp):
        """

        :return:
        """
        from_address = event.get(TransactionConstant.from_address)
        to_address = event.get(TransactionConstant.to_address)
        tx_id = event.get(TransactionConstant.transaction_hash)
        timestamp = timestamp
        token = event.get(EventConstant.contract_address)
        value = event.get(TransactionConstant.value)

        value_usd = self.price_service.token_amount_to_usd(token, value)
        transfer = Transfer(tx_id, timestamp, from_address, to_address, token, value, value_usd)

        daily_timestamp = round_timestamp_to_date(timestamp)

        """
        dailyTransactionAmounts: Tổng giá trị giao dịch của wallet trong 100 ngày, mảng gồm 100 ngày - lưu ý chỉ tính giao dịch chuyển tiền tới tài khoản này
        """

        daily_transaction_amount = self.klg_database.get_daily_transaction_amount_100(to_address)
        current_value_timestamp = to_float(daily_transaction_amount.get(daily_timestamp))
        value_usd = self.price_service.token_amount_to_usd(token, value)
        daily_transaction_amount[daily_timestamp] = current_value_timestamp + value_usd
        self.klg_database.update_daily_transaction_amount_100(to_address, daily_transaction_amount)

        self.klg_database.create_transfer_relationship(transfer)

    def _deposit_handler(self, event, timestamp):
        """

        :return:
        """
        from_address = event.get(DepositEventConstant.onBehalfOf)
        to_address = event.get(EventConstant.contract_address)
        tx_id = event.get(TransactionConstant.transaction_hash)
        timestamp = timestamp
        token = event.get(DepositEventConstant.reserve)
        value = event.get(DepositEventConstant.amount)

        value_usd = self.price_service.token_amount_to_usd(token, value)
        related_wallets = event.get(TransactionConstant.related_wallets)
        deposit = Deposit(tx_id, timestamp, from_address, to_address, token, value, value_usd, related_wallets)
        self.klg_database.create_deposit_relationship(deposit)

    def _mint_handler(self, event, timestamp):
        """

        :return:
        """
        from_address = event.get(MintEventConstant.minter)
        to_address = event.get(EventConstant.contract_address)
        tx_id = event.get(TransactionConstant.transaction_hash)
        timestamp = timestamp
        token = event.get(EventConstant.contract_address)
        value = event.get(MintEventConstant.mintAmount)

        value_usd = self.price_service.token_amount_to_usd(token, value)
        related_wallets = event.get(TransactionConstant.related_wallets)
        deposit = Deposit(tx_id, timestamp, from_address, to_address, token, value, value_usd, related_wallets)
        self.klg_database.create_deposit_relationship(deposit)

    def _borrow_handler(self, event, timestamp):
        """

        :return:
        """
        tx_id = event.get(TransactionConstant.transaction_hash)
        timestamp = timestamp
        to_address = event.get(EventConstant.contract_address)
        from_address = event.get(BorrowEventVTokenConstant.borrower)
        if not from_address:
            from_address = event.get(BorrowEventPoolConstant.user)
            token = event.get(BorrowEventPoolConstant.reserve)
            value = event.get(BorrowEventPoolConstant.amount)
        else:
            token = event.get(EventConstant.contract_address)
            value = event.get(BorrowEventVTokenConstant.borrowAmount)

        value_usd = self.price_service.token_amount_to_usd(token, value)
        related_wallets = event.get(TransactionConstant.related_wallets)
        borrow = Borrow(tx_id, timestamp, from_address, to_address, token, value, value_usd, related_wallets)
        self.klg_database.create_borrow_relationship(borrow)

    def _redeem_handler(self, event, timestamp):
        """

        :return:
        """
        tx_id = event.get(TransactionConstant.transaction_hash)
        timestamp = timestamp
        to_address = event.get(EventConstant.contract_address)
        from_address = event.get(RedeemEventConstant.redeemer)
        token = event.get(EventConstant.contract_address)
        value = event.get(RedeemEventConstant.redeemAmount)

        value_usd = self.price_service.token_amount_to_usd(token, value)
        related_wallets = event.get(TransactionConstant.related_wallets)
        withdraw = Withdraw(tx_id, timestamp, from_address, to_address, token, value, value_usd, related_wallets)
        self.klg_database.create_withdraw_relationship(withdraw)

    def _withdraw_handler(self, event, timestamp):
        """

        :return:
        """
        tx_id = event.get(TransactionConstant.transaction_hash)
        timestamp = timestamp
        to_address = event.get(EventConstant.contract_address)
        from_address = event.get(WithdrawEventConstant.user)
        token = event.get(WithdrawEventConstant.reserve)
        value = event.get(WithdrawEventConstant.amount)
        value_usd = self.price_service.token_amount_to_usd(token, value)
        related_wallets = event.get(TransactionConstant.related_wallets)
        withdraw = Withdraw(tx_id, timestamp, from_address, to_address, token, value, value_usd, related_wallets)
        self.klg_database.create_withdraw_relationship(withdraw)

    def _repay_handler(self, event, timestamp):
        """

        :return:
        """
        tx_id = event.get(TransactionConstant.transaction_hash)
        timestamp = timestamp
        to_address = event.get(EventConstant.contract_address)
        from_address = event.get(RepayEventConstant.user)
        token = event.get(RepayEventConstant.reserve)
        value = event.get(RepayEventConstant.amount)

        value_usd = self.price_service.token_amount_to_usd(token, value)
        related_wallets = event.get(TransactionConstant.related_wallets)
        repay = Repay(tx_id, timestamp, from_address, to_address, token, value, value_usd, related_wallets)
        self.klg_database.create_repay_relationship(repay)

    def _repay_borrow_handler(self, event, timestamp):
        """

        :return:
        """
        tx_id = event.get(TransactionConstant.transaction_hash)
        timestamp = timestamp
        to_address = event.get(EventConstant.contract_address)
        from_address = event.get(RepayBorrowEventConstant.borrower)
        token = event.get(EventConstant.contract_address)
        value = event.get(RepayBorrowEventConstant.repayAmount)

        value_usd = self.price_service.token_amount_to_usd(token, value)
        related_wallets = event.get(TransactionConstant.related_wallets)
        repay = Repay(tx_id, timestamp, from_address, to_address, token, value, value_usd, related_wallets)
        self.klg_database.create_repay_relationship(repay)

    def _liquidate_borrow_handler(self, event, timestamp):
        """

        :return:
        """
        tx_id = event.get(TransactionConstant.transaction_hash)
        timestamp = timestamp
        protocol = event.get(EventConstant.contract_address)

        from_wallet = event.get(LiquidateBorrowEventConstant.liquidator)
        to_wallet = event.get(LiquidateBorrowEventConstant.borrower)
        wallets = event.get(TransactionConstant.related_wallets)
        from_balance, from_amount, to_balance, to_amount = [], [], [], []
        for wallet in wallets:
            if from_wallet == wallet.get(WalletConstant.address):
                balance = wallet.get(WalletConstant.balance)
                from_balance, from_amount = dict_to_two_list(balance)
            if to_wallet == wallet.get(WalletConstant.address):
                balance = wallet.get(WalletConstant.balance)
                to_balance, to_amount = dict_to_two_list(balance)

        """
        Cập nhật
        numberOfLiquidation: số lần bị thanh lý khoản vay
        totalAmountOfLiquidation: tổng số tiền bị thanh lý khoản vay từ trước đến nay
        """
        self.klg_database.update_num_of_liquidation_100(to_wallet, 1)

        seizeTokens = event.get(LiquidateBorrowEventConstant.seizeTokens)
        token_address = protocol
        value_usd = self.price_service.token_amount_to_usd(token_address, seizeTokens)
        self.klg_database.update_total_amount_liquidation_100(to_wallet, value_usd)
        liquidate = Liquidate(
            transactionID=tx_id,
            timestamp=timestamp,
            protocol=protocol,
            fromWallet=from_wallet,
            toWallet=to_wallet,
            fromBalance=from_balance,
            fromAmount=from_amount,
            toBalance=to_balance,
            toAmount=to_amount)
        self.klg_database.create_liquidate_relationship(liquidate)

    def liquidate_call_handler(self, event, timestamp):
        """

        :return:
        """
        tx_id = event.get(TransactionConstant.transaction_hash)
        timestamp = timestamp
        protocol = event.get(EventConstant.contract_address)

        from_wallet = event.get(LiquidationCallEventConstant.liquidator)
        to_wallet = event.get(LiquidationCallEventConstant.user)
        wallets = event.get(TransactionConstant.related_wallets)
        from_balance, from_amount, to_balance, to_amount = [], [], [], []
        for wallet in wallets:
            if from_wallet == wallet.get(WalletConstant.address):
                balance = wallet.get(WalletConstant.balance)
                from_balance, from_amount = dict_to_two_list(balance)
            if to_wallet == wallet.get(WalletConstant.address):
                balance = wallet.get(WalletConstant.balance)
                to_balance, to_amount = dict_to_two_list(balance)

        """
        Cập nhật
        numberOfLiquidation: số lần bị thanh lý khoản vay
        totalAmountOfLiquidation: tổng số tiền bị thanh lý khoản vay từ trước đến nay
        """
        self.klg_database.update_num_of_liquidation_100(to_wallet, 1)
        token_address = event.get(LiquidationCallEventConstant.debtAsset)
        amount = event.get(LiquidationCallEventConstant.liquidatedCollateralAmount)
        value_usd = self.price_service.token_amount_to_usd(token_address, amount)
        self.klg_database.update_total_amount_liquidation_100(to_wallet, value_usd)

        liquidate = Liquidate(
            transactionID=tx_id,
            timestamp=timestamp,
            protocol=protocol,
            fromWallet=from_wallet,
            toWallet=to_wallet,
            fromBalance=from_balance,
            fromAmount=from_amount,
            toBalance=to_balance,
            toAmount=to_amount)
        self.klg_database.create_liquidate_relationship(liquidate)
