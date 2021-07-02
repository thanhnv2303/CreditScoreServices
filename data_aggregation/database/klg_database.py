import logging
import time

from py2neo import Graph

from config.config import Neo4jConfig
from config.constant import WalletConstant
from data_aggregation.database.neo4j_services_db.cypher_transfer import RelationshipType
from data_aggregation.database.relationships_model import Liquidate, Withdraw, Repay, Borrow, Deposit, Transfer
from data_aggregation.services.update_graph_relationship import update_info_merge_relationship, \
    update_token_balance_relationship
from services.zip_service import two_list_to_dict, dict_to_two_list

logger = logging.getLogger("KlgDatabase")


class KlgDatabase(object):
    """Manages connection to  database_common and makes async queries
    """

    def __init__(self):
        self._conn = None

        bolt = f"bolt://{Neo4jConfig.HOST}:{Neo4jConfig.BOTH_PORT}"
        self._graph = Graph(bolt, auth=(Neo4jConfig.NEO4J_USERNAME, Neo4jConfig.NEO4J_PASSWORD))

    def _create_index(self):
        self._graph.run("")

    def update_wallet_token(self, wallet_address, token_map={}, balance=0):
        # """
        # cập nhật  trường
        # Token:[map trong neo4j_services_db] ghi lại các loại token và số lượng từng loại có trong wallet.
        # Key: token address - value: số lượng token mà wallet đang giữ
        # Balance:Giá trị tài khoản = Σ(value_token*token_price)(USD)
        # :return:
        # """

        tokens, tokenBalances = dict_to_two_list(token_map)

        # match = self._graph.run("MATCH (p:Wallet { address : $address}) return p ", address=wallet_address).data()
        # if not match:
        #     create = self._graph.run("MERGE (p:Wallet { address: $address }) "
        #                              "SET p.tokens = $tokens, "
        #                              "p.tokenBalances = $tokenBalances, "
        #                              "p.balanceInUSD = $balanceInUSD "
        #                              "RETURN p",
        #                              address=wallet_address,
        #                              tokens=tokens,
        #                              tokenBalances=tokenBalances,
        #                              balanceInUSD=balance).data()
        # else:
        #     create = self._graph.run("MATCH (p:Wallet { address: $address }) "
        #                              "SET p.tokens = $tokens, "
        #                              "p.tokenBalances = $tokenBalances, "
        #                              "p.balanceInUSD = $balanceInUSD "
        #                              "RETURN p",
        #                              address=wallet_address,
        #                              tokens=tokens,
        #                              tokenBalances=tokenBalances,
        #                              balanceInUSD=balance).data()
        create = self._graph.run("MERGE (p:Wallet { address: $address }) "
                                 "SET p.tokens = $tokens, "
                                 "p.tokenBalances = $tokenBalances, "
                                 "p.balanceInUSD = $balanceInUSD "
                                 "RETURN p",
                                 address=wallet_address,
                                 tokens=tokens,
                                 tokenBalances=tokenBalances,
                                 balanceInUSD=balance).data()
        return create[0]["p"]

    def update_wallet_token_deposit_and_borrow(self, wallet_address, token_deposit_map={}, token_borrow_map={},
                                               deposit=0, borrow=0):
        # """
        # cập nhật các trường

        # Token_Deposit: [map trong neo4j_services_db] ghi lại các loại token và số lượng từng loại mà wallet deposit.
        # Key: token address - value: số lượng token mà wallet đang deposit

        # &&
        # Token_Borrow: [map trong neo4j_services_db] ghi lại các loại token và số lượng từng loại mà wallet borrow.
        # Key: token address - value: số lượng token mà wallet đang borrow

        # &&
        # Deposit(USD): lượng tiền đang deposit ở các lending pool - tính ra USD

        # $$
        # Borrow: lượng tiền đang đi vay ở các lending pool -(USD)
        # :return:
        # """
        depositTokens, depositTokenBalances = dict_to_two_list(token_deposit_map)
        borrowTokens, borrowTokenBalances = dict_to_two_list(token_borrow_map)

        # match = self._graph.run("MATCH (p:Wallet { address : $address}) return p ", address=wallet_address).data()
        # if not match:
        #     create = self._graph.run("MERGE (p:Wallet { address: $address }) "
        #                              "SET p.depositTokens = $depositTokens, "
        #                              "p.depositTokenBalances = $depositTokenBalances, "
        #                              "p.borrowTokens = $borrowTokens, "
        #                              "p.borrowTokenBalances = $borrowTokenBalances, "
        #                              "p.depositInUSD = $depositInUSD, "
        #                              "p.borrowInUSD = $borrowInUSD "
        #                              "RETURN p",
        #                              address=wallet_address,
        #                              depositTokens=depositTokens,
        #                              depositTokenBalances=depositTokenBalances,
        #                              borrowTokens=borrowTokens,
        #                              borrowTokenBalances=borrowTokenBalances,
        #                              depositInUSD=deposit,
        #                              borrowInUSD=borrow).data()
        # else:
        #     create = self._graph.run("MATCH (p:Wallet { address: $address }) "
        #                              "SET p.depositTokens = $depositTokens, "
        #                              "p.depositTokenBalances = $depositTokenBalances, "
        #                              "p.borrowTokens = $borrowTokens, "
        #                              "p.borrowTokenBalances = $borrowTokenBalances, "
        #                              "p.depositInUSD = $depositInUSD, "
        #                              "p.borrowInUSD = $borrowInUSD "
        #                              "RETURN p",
        #                              address=wallet_address,
        #                              depositTokens=depositTokens,
        #                              depositTokenBalances=depositTokenBalances,
        #                              borrowTokens=borrowTokens,
        #                              borrowTokenBalances=borrowTokenBalances,
        #                              depositInUSD=deposit,
        #                              borrowInUSD=borrow).data()
        start_time = time.time()
        create = self._graph.run("MERGE (p:Wallet { address: $address }) "
                                 "SET p.depositTokens = $depositTokens, "
                                 "p.depositTokenBalances = $depositTokenBalances, "
                                 "p.borrowTokens = $borrowTokens, "
                                 "p.borrowTokenBalances = $borrowTokenBalances, "
                                 "p.depositInUSD = $depositInUSD, "
                                 "p.borrowInUSD = $borrowInUSD "
                                 "RETURN p",
                                 address=wallet_address,
                                 depositTokens=depositTokens,
                                 depositTokenBalances=depositTokenBalances,
                                 borrowTokens=borrowTokens,
                                 borrowTokenBalances=borrowTokenBalances,
                                 depositInUSD=deposit,
                                 borrowInUSD=borrow).data()
        logger.info(f"Time to update deposit token and deposit token balances is {time.time() - start_time}")
        return create[0]["p"]

    def update_wallet_created_at(self, wallet_address, created_at):
        # """
        # cập nhật các trường

        # Created at: timestamp mà wallet thực hiện giao dịch đầu tiên trên bsc
        # :return:
        # """
        # pass
        # match = self._graph.run("MATCH (p:Wallet { address : $address}) return p ", address=wallet_address).data()
        # if not match:
        #     create = self._graph.run("MERGE (p:Wallet { address: $address }) "
        #                              "SET p.createdAt = $createdAt "
        #                              "RETURN p",
        #                              address=wallet_address, createdAt=created_at).data()
        # else:
        #     create = self._graph.run("MATCH (p:Wallet { address: $address }) "
        #                              "SET p.createdAt = $createdAt "
        #                              "RETURN p",
        #                              address=wallet_address, createdAt=created_at).data()
        start_time = time.time()
        create = self._graph.run("MERGE (p:Wallet { address: $address }) "
                                 "SET p.createdAt = $createdAt "
                                 "RETURN p",
                                 address=wallet_address, createdAt=created_at).data()
        logger.info(f"Time time to update create at {time.time() - start_time}")
        return create[0]["p"]

    def get_wallet_created_at(self, wallet_address):
        getter = self._graph.run("match (p:Wallet { address: $address }) return p.createdAt ",
                                 address=wallet_address).data()
        if getter and getter[0]["p.createdAt"]:
            return getter[0]["p.createdAt"]
        return None

    def get_balance_100(self, wallet_address):
        # """
        # Lấy ra
        # balance100 : [map ] ghi lại các lần balance thay đổi trong 100 ngày qua -
        # key: timestamp(các timestamp thay đổi balance) - value: giá trị tại balance timestamp đó

        # :param wallet_address:
        # :return: balance100: dict()
        # """
        keys_list = []  # timestamp_balance_100 list
        values_list = []  # balance_100 list
        start_time = time.time()
        try:
            getter = self._graph.run("match (p:Wallet { address: $address }) return p.balanceChangeLogTimestamps ",
                                     address=wallet_address).data()
            if getter and getter[0]["p.balanceChangeLogTimestamps"]:
                keys_list = keys_list + getter[0]["p.balanceChangeLogTimestamps"]

            getter = self._graph.run("match (p:Wallet { address: $address }) return p.balanceChangeLogValues ",
                                     address=wallet_address).data()
            if getter and getter[0]["p.balanceChangeLogValues"]:
                values_list = values_list + getter[0]["p.balanceChangeLogValues"]
            logger.info(f"time to get balance 100 {time.time() - start_time}")
        except Exception as e:
            logger.info("get_balance_100")
            logger.error(e)
        return two_list_to_dict(keys_list, values_list)

    def update_balance_100(self, wallet_address, balance_100):
        # """
        # cập nhật
        # balance100 : [map ] ghi lại các lần balance thay đổi trong 100 ngày qua -
        # key: timestamp(các timestamp thay đổi balance) - value: giá trị tại balance timestamp đó

        # :param wallet_address:
        # :return:
        # """
        start_time = time.time()
        if not wallet_address or not balance_100:
            return
        keys, values = dict_to_two_list(balance_100)
        keys = list(keys)
        values = list(values)
        # match = self._graph.run("MATCH (p:Wallet { address : $address}) return p ", address=wallet_address).data()
        # if not match:
        #     create = self._graph.run("MERGE (p:Wallet { address: $address }) "
        #                              "SET p.balanceChangeLogTimestamps = $balance100, "
        #                              "p.balanceChangeLogValues = $balance100value "
        #                              "RETURN p",
        #                              address=wallet_address, balance100=keys, balance100value=values).data()
        # else:
        #     create = self._graph.run("MATCH (p:Wallet { address: $address }) "
        #                              "SET p.balanceChangeLogTimestamps = $balance100, "
        #                              "p.balanceChangeLogValues = $balance100value "
        #                              "RETURN p",
        #                              address=wallet_address, balance100=keys, balance100value=values).data()
        create = self._graph.run("MERGE (p:Wallet { address: $address }) "
                                 "SET p.balanceChangeLogTimestamps = $balance100, "
                                 "p.balanceChangeLogValues = $balance100value "
                                 "RETURN p",
                                 address=wallet_address, balance100=keys, balance100value=values).data()
        logger.info(f"Time to update balance 100 {time.time() - start_time}")
        return create[0]["p"]

    def get_daily_transaction_amount_100(self, wallet_address):
        keys_list = []  # timestamp_balance_100 list
        values_list = []  # balance_100 list
        start_time = time.time()
        try:
            getter = self._graph.run(
                "match (p:Wallet { address: $address }) return p.dailyTransactionAmountsTimestamp ",
                address=wallet_address).data()
            if getter and getter[0]["p.dailyTransactionAmountsTimestamp"]:
                keys_list = keys_list + getter[0]["p.dailyTransactionAmountsTimestamp"]

            getter = self._graph.run("match (p:Wallet { address: $address }) return p.dailyTransactionAmounts ",
                                     address=wallet_address).data()
            if getter and getter[0]["p.dailyTransactionAmounts"]:
                values_list = values_list + getter[0]["p.dailyTransactionAmounts"]
            logger.info(f"time to get get_daily_transaction_amount_100 {time.time() - start_time}")
        except Exception as e:
            logger.info("get_daily_transaction_amount_100")
            logger.error(e)
        return two_list_to_dict(keys_list, values_list)

    def update_daily_transaction_amount_100(self, wallet_address, transaction_amount):
        # """
        # cập nhật
        # dailyTransactionAmounts: Tổng giá trị giao dịch của wallet trong 100 ngày, mảng gồm 100 ngày - lưu ý chỉ tính giao dịch chuyển tiền tới tài khoản này
        #

        # :param wallet_address:
        # :return:
        # """
        # if not wallet_address or not transaction_amount:
        #     return
        # create = self._graph.run("MERGE (p:Wallet { address: $address }) "
        #                          "SET p.dailyTransactionAmounts = coalesce(p.dailyTransactionAmounts, []) + $dailyTransactionAmounts "
        #                          "RETURN p",
        #                          address=wallet_address, dailyTransactionAmounts=transaction_amount).data()

        start_time = time.time()
        if not wallet_address or not transaction_amount:
            return
        keys, values = dict_to_two_list(transaction_amount)
        keys = list(keys)
        values = list(values)
        create = self._graph.run("MERGE (p:Wallet { address: $address }) "
                                 "SET p.dailyTransactionAmountsTimestamp = $dailyTransactionAmountsTimestamp, "
                                 "p.dailyTransactionAmounts = $dailyTransactionAmounts "
                                 "RETURN p",
                                 address=wallet_address, dailyTransactionAmountsTimestamp=keys,
                                 dailyTransactionAmounts=values).data()
        logger.info(f"Time to update balance 100 {time.time() - start_time}")
        return create[0]["p"]

    def get_daily_daily_frequency_of_transaction(self, wallet_address):
        keys_list = []  # timestamp_balance_100 list
        values_list = []  # balance_100 list
        start_time = time.time()
        try:
            getter = self._graph.run(
                "match (p:Wallet { address: $address }) return p.dailyFrequencyOfTransactionsTimestamp ",
                address=wallet_address).data()
            if getter and getter[0]["p.dailyFrequencyOfTransactionsTimestamp"]:
                keys_list = keys_list + list(getter[0]["p.dailyFrequencyOfTransactionsTimestamp"])

            getter = self._graph.run("match (p:Wallet { address: $address }) return p.dailyFrequencyOfTransactions ",
                                     address=wallet_address).data()
            if getter and getter[0]["p.dailyFrequencyOfTransactions"]:
                values_list = values_list + getter[0]["p.dailyFrequencyOfTransactions"]
        except Exception as e:
            logger.info("get_daily_daily_frequency_of_transaction")
            logger.error(e)
        logger.info(f"time to get get_daily_transaction_amount_100 {time.time() - start_time}")
        return two_list_to_dict(keys_list, values_list)

    def update_daily_frequency_of_transaction(self, wallet_address, transaction_id):
        # """
        # cập nhật
        # dailyTransactionAmounts: Tổng giá trị giao dịch của wallet trong 100 ngày, mảng gồm 100 ngày - lưu ý chỉ tính giao dịch chuyển tiền tới tài khoản này
        #

        # :param wallet_address:
        # :return:
        # """
        if not wallet_address or not transaction_id:
            return
        start_time = time.time()
        # create = self._graph.run("MERGE (p:Wallet { address: $address }) "
        #                          "SET p.dailyFrequencyOfTransactions = coalesce(p.dailyFrequencyOfTransactions, []) + $dailyFrequencyOfTransactions "
        #                          "RETURN p",
        #                          address=wallet_address, dailyFrequencyOfTransactions=transaction_id).data()
        keys, values = dict_to_two_list(transaction_id)
        keys = list(keys)
        values = list(values)
        create = self._graph.run("MERGE (p:Wallet { address: $address }) "
                                 "SET p.dailyFrequencyOfTransactionsTimestamp = $dailyFrequencyOfTransactionsTimestamp, "
                                 "p.dailyFrequencyOfTransactions = $dailyFrequencyOfTransactions "
                                 "RETURN p",
                                 address=wallet_address, dailyFrequencyOfTransactionsTimestamp=keys,
                                 dailyFrequencyOfTransactions=values).data()
        logger.info(f"Time to update update_daly_frequency_of_transaction {time.time() - start_time}")
        return create[0]["p"]

    def get_num_of_liquidation_100(self, wallet_address):
        # """
        # Lấy ra
        # numberOfLiquidation: số lần bị thanh lý khoản vay

        # :param wallet_address:
        # :return:
        # """
        number = 0

        getter = self._graph.run("match (p:Wallet { address: $address }) return p.numberOfLiquidation ",
                                 address=wallet_address).data()
        if getter and getter[0]["p.numberOfLiquidation"]:
            number = getter[0]["p.numberOfLiquidation"]

        return number

    def update_num_of_liquidation_100(self, wallet_address, number):
        # """
        # cập nhật
        # numberOfLiquidation: số lần bị thanh lý khoản vay
        #

        # :param wallet_address:
        # :return:
        # """
        if not wallet_address or not number:
            return
        create = self._graph.run("MERGE (p:Wallet { address: $address }) "
                                 "ON CREATE SET p.numberOfLiquidation = 1 "
                                 "ON MATCH SET p.numberOfLiquidation = p.numberOfLiquidation + $numberOfLiquidation "
                                 "RETURN p",
                                 address=wallet_address, numberOfLiquidation=number).data()
        return create[0]["p"]

    def get_total_amount_liquidation_100(self, wallet_address):
        # """
        # Lấy ra
        # numberOfLiquidation: số lần bị thanh lý khoản vay

        # :param wallet_address:
        # :return:
        # """
        number = 0

        getter = self._graph.run("match (p:Wallet { address: $address }) return p.totalAmountOfLiquidation ",
                                 address=wallet_address).data()
        if getter and getter[0]["p.totalAmountOfLiquidation"]:
            number = getter[0]["p.totalAmountOfLiquidation"]

        return number

    def update_total_amount_liquidation_100(self, wallet_address, number):
        # """
        # cập nhật
        # numberOfLiquidation: số lần bị thanh lý khoản vay
        #

        # :param wallet_address:
        # :return:
        # """
        if not wallet_address or not number:
            return
        create = self._graph.run("MERGE (p:Wallet { address: $address }) "
                                 "ON CREATE SET p.totalAmountOfLiquidation = 0 "
                                 "ON MATCH SET p.totalAmountOfLiquidation = p.totalAmountOfLiquidation + $totalAmountOfLiquidation "
                                 "RETURN p",
                                 address=wallet_address, totalAmountOfLiquidation=number).data()
        return create[0]["p"]

    def get_deposit_100(self, wallet_address):
        # """
        # Lấy ra
        # deposit100: [map trong neo4j_services_db] ghi lại các lần lượng deposit thay đổi trong 100 ngày qua -
        # key: timestamp(các timestamp thay đổi deposit) - value: giá trị deposit tại timestamp đó

        # :param wallet_address:
        # :return:
        # """
        keys_list = []  # timestamp_deposit_100 list
        values_list = []  # deposit_100 list

        getter = self._graph.run("match (p:Wallet { address: $address }) return p.depositChangeLogTimestamps ",
                                 address=wallet_address).data()
        if getter and getter[0]["p.depositChangeLogTimestamps"]:
            keys_list = getter[0]["p.depositChangeLogTimestamps"]

        getter = self._graph.run("match (p:Wallet { address: $address }) return p.depositChangeLogValues ",
                                 address=wallet_address).data()
        if getter and getter[0]["p.depositChangeLogValues"]:
            values_list = getter[0]["p.depositChangeLogValues"]

        return two_list_to_dict(keys_list, values_list)

    def update_deposit_100(self, wallet_address, deposit_100):
        # """
        # cập nhật
        # deposit100: [map trong neo4j_services_db] ghi lại các lần lượng deposit thay đổi trong 100 ngày qua -
        # key: timestamp(các timestamp thay đổi deposit) - value: giá trị deposit tại timestamp đó

        # :param wallet_address:
        # :return:
        # """
        if not wallet_address or not deposit_100:
            return
        keys, values = dict_to_two_list(deposit_100)
        keys = list(keys)
        values = list(values)
        # match = self._graph.run("MATCH (p:Wallet { address : $address}) return p ", address=wallet_address).data()
        # if not match:
        #     create = self._graph.run("MERGE (p:Wallet { address: $address }) "
        #                              "SET p.depositChangeLogTimestamps = $deposit100, "
        #                              "p.depositChangeLogValues = $deposit100value "
        #                              "RETURN p",
        #                              address=wallet_address, deposit100=keys, deposit100value=values).data()
        # else:
        #     create = self._graph.run("MATCH (p:Wallet { address: $address }) "
        #                              "SET p.depositChangeLogTimestamps = $deposit100, "
        #                              "p.depositChangeLogValues = $deposit100value "
        #                              "RETURN p",
        #                              address=wallet_address, deposit100=keys, deposit100value=values).data()
        create = self._graph.run("MERGE (p:Wallet { address: $address }) "
                                 "SET p.depositChangeLogTimestamps = $deposit100, "
                                 "p.depositChangeLogValues = $deposit100value "
                                 "RETURN p",
                                 address=wallet_address, deposit100=keys, deposit100value=values).data()
        return create[0]["p"]

    def get_borrow_100(self, wallet_address):
        # """
        # Lấy ra
        # balance100 : [map trong neo4j_services_db] ghi lại các lần balance thay đổi trong 100 ngày qua -
        # key: timestamp(các timestamp thay đổi balance) - value: giá trị tại balance timestamp đó

        # :param wallet_address:
        # :return:
        # """
        keys_list = []  # timestamp_borrow_100 list
        values_list = []  # borrow_100 list

        getter = self._graph.run("match (p:Wallet { address: $address }) return p.borrowChangeLogTimestamps ",
                                 address=wallet_address).data()
        if getter and getter[0]["p.borrowChangeLogTimestamps"]:
            keys_list = getter[0]["p.borrowChangeLogTimestamps"]

        getter = self._graph.run("match (p:Wallet { address: $address }) return p.borrowChangeLogValues ",
                                 address=wallet_address).data()
        if getter and getter[0]["p.borrowChangeLogValues"]:
            values_list = getter[0]["p.borrowChangeLogValues"]

        return two_list_to_dict(keys_list, values_list)

    def update_borrow_100(self, wallet_address, borrow_100):
        # """
        # cập nhật
        # borrow100: [map trong neo4j_services_db] ghi lại các lần lượng borrow thay đổi trong 100 ngày qua -
        # key: timestamp(các timestamp thay đổi borrow) - value: giá trị borrow tại timestamp đó

        # :param wallet_address:
        # :return:
        # """
        if not wallet_address or not borrow_100:
            return
        keys, values = dict_to_two_list(borrow_100)
        keys = list(keys)
        values = list(values)

        # match = self._graph.run("MATCH (p:Wallet { address : $address}) return p ", address=wallet_address).data()
        # if not match:
        #     create = self._graph.run("MERGE (p:Wallet { address: $address }) "
        #                              "SET p.borrowChangeLogTimestamps = $borrow100, "
        #                              "p.borrowChangeLogValues = $borrow100value "
        #                              "RETURN p",
        #                              address=wallet_address, borrow100=keys, borrow100value=values).data()
        # else:
        #     create = self._graph.run("MATCH (p:Wallet { address: $address }) "
        #                              "SET p.borrowChangeLogTimestamps = $borrow100, "
        #                              "p.borrowChangeLogValues = $borrow100value "
        #                              "RETURN p",
        #                              address=wallet_address, borrow100=keys, borrow100value=values).data()
        create = self._graph.run("MERGE (p:Wallet { address: $address }) "
                                 "SET p.borrowChangeLogTimestamps = $borrow100, "
                                 "p.borrowChangeLogValues = $borrow100value "
                                 "RETURN p",
                                 address=wallet_address, borrow100=keys, borrow100value=values).data()
        return create[0]["p"]

    def update_daily_frequency_of_transactions(self, wallet_address, daily_frequency_of_transactions):
        """

        dailyFrequencyOfTransactions : Số lần giao dịch của token này trong 100 ngày gần

        :param wallet_address:
        :param daily_frequency_of_transactions:
        :return:
        """
        if not wallet_address or not daily_frequency_of_transactions:
            return

        create = self._graph.run("MERGE (p { address: $address }) "
                                 "SET p.dailyFrequencyOfTransactions = $dailyFrequencyOfTransactions "
                                 "RETURN p",
                                 address=wallet_address,
                                 dailyFrequencyOfTransactions=daily_frequency_of_transactions).data()
        return create[0]["p"]

    def create_transfer_relationship(self, transfer: Transfer):
        value_usd = transfer.valueUsd
        from_address = transfer.fromWallet
        to_address = transfer.toWallet
        timestamp = transfer.timestamp
        relationship_type = RelationshipType.TRANSFER
        total_number, total_amount, highest_value, lowest_value, avg_value, median, sort_values, tokens = update_info_merge_relationship(
            self._graph, from_address, to_address, value_usd, relationship_type)

        merge = self._graph.run("""
                                MATCH (a { address: $fromWallet }), (b {address: $toWallet}) 
                                MERGE (a)-[r:TRANSFERS ]->(b)
                                SET r.transferLogTimestamps = coalesce(r.transferLogTimestamps, []) + $transferLogTimestamp,
                                    r.transferLogValues = coalesce(r.transferLogValues, []) + $transferLogValue,
                                    r.totalNumberOfTransfer = $totalNumberOfTransfer,
                                    r.totalAmountOfTransferInUSD= $totalAmountOfTransferInUSD,
                                    r.highestValueTransferInUSD = $highestValueTransferInUSD,
                                    r.lowestValueTransferInUSD = $lowestValueTransferInUSD,
                                    r.averageValueTransferInUSD = $averageValueTransferInUSD,
                                    r.medianValueTransferInUSD = $medianValueTransferInUSD,                
                                    r.sortValues = $sortValues              
                                """,
                                fromWallet=from_address,
                                toWallet=to_address,
                                transferLogTimestamp=timestamp,
                                transferLogValue=value_usd,
                                totalNumberOfTransfer=total_number,
                                totalAmountOfTransferInUSD=total_amount,
                                highestValueTransferInUSD=highest_value,
                                lowestValueTransferInUSD=lowest_value,
                                averageValueTransferInUSD=avg_value,
                                medianValueTransferInUSD=median,
                                sortValues=sort_values,
                                ).data()
        return merge

    def create_deposit_relationship(self, deposit: Deposit):

        value_usd = deposit.valueUsd
        from_address = deposit.fromWallet
        to_address = deposit.toAddress
        timestamp = deposit.timestamp
        total_number, total_amount, highest_value, lowest_value, avg_value, median, sort_values, tokens = update_info_merge_relationship(
            self._graph, from_address, to_address, value_usd, RelationshipType.DEPOSIT)

        token = deposit.token
        current_tokens = tokens
        wallet_address = from_address
        related_wallets = deposit.relatedWallets
        balance_type = WalletConstant.supply
        current_tokens, tokens_amount = update_token_balance_relationship(token, current_tokens, wallet_address,
                                                                          related_wallets, balance_type)

        merge = self._graph.run("""
                                       MATCH (a { address: $fromWallet }), (b {address: $toWallet}) 
                                       MERGE (a)-[r:DEPOSITS ]->(b)
                                       SET r.depositLogTimestamps = coalesce(r.depositLogTimestamps, []) + $depositLogTimestamps,
                                           r.depositLogValues = coalesce(r.depositLogValues, []) + $depositLogValues,
                                           r.totalNumberOfDeposit = $totalNumberOfDeposit,
                                           r.totalAmountOfDepositInUSD= $totalAmountOfDepositInUSD,
                                           r.highestDepositInUSD = $highestDepositInUSD,
                                           r.lowestValueBorrowInUSD = $lowestValueBorrowInUSD,
                                           r.averageDepositInUSD = $averageDepositInUSD,
                                           r.medianDepositInUSD = $medianDepositInUSD,                
                                           r.sortValues = $sortValues ,
                                           r.depositTokens = $depositTokens,
                                           r.depositTokenBalances = $depositTokenBalances,
                                           r.tokens = $tokens  
                                        RETURN r           
                                       """,
                                fromWallet=from_address,
                                toWallet=to_address,
                                depositLogTimestamps=timestamp,
                                depositLogValues=value_usd,
                                totalNumberOfDeposit=total_number,
                                totalAmountOfDepositInUSD=total_amount,
                                highestDepositInUSD=highest_value,
                                lowestValueBorrowInUSD=lowest_value,
                                averageDepositInUSD=avg_value,
                                medianDepositInUSD=median,
                                sortValues=sort_values,
                                depositTokens=current_tokens,
                                depositTokenBalances=tokens_amount,
                                tokens=current_tokens
                                ).data()
        return merge

    def create_borrow_relationship(self, borrow: Borrow):
        value_usd = borrow.valueUsd
        from_address = borrow.fromWallet
        to_address = borrow.toAddress
        timestamp = borrow.timestamp
        total_number, total_amount, highest_value, lowest_value, avg_value, median, sort_values, tokens = update_info_merge_relationship(
            self._graph, from_address, to_address, value_usd, RelationshipType.BORROW)

        token = borrow.token
        current_tokens = tokens
        wallet_address = from_address
        related_wallets = borrow.relatedWallets
        balance_type = WalletConstant.borrow
        current_tokens, tokens_amount = update_token_balance_relationship(token, current_tokens, wallet_address,
                                                                          related_wallets, balance_type)

        merge = self._graph.run("""
                                               MATCH (a { address: $fromWallet }), (b {address: $toWallet}) 
                                               MERGE (a)-[r:BORROWS ]->(b)
                                               SET r.borrowLogTimestamps = coalesce(r.borrowLogTimestamps, []) + $borrowLogTimestamps,
                                                   r.borrowLogValues = coalesce(r.borrowLogValues, []) + $borrowLogValues,
                                                   r.totalNumberOfBorrow = $totalNumberOfBorrow,
                                                   r.totalAmountOfBorrowInUSD= $totalAmountOfBorrowInUSD,
                                                   r.highestBorrowInUSD = $highestBorrowInUSD,
                                                   r.lowestValueBorrowInUSD = $lowestValueBorrowInUSD,
                                                   r.averageBorrowInUSD = $averageBorrowInUSD,
                                                   r.medianBorrowInUSD = $medianBorrowInUSD,                
                                                   r.sortValues = $sortValues ,
                                                   r.borrowTokens = $borrowTokens,
                                                   r.borrowTokenBalances = $borrowTokenBalances,
                                                   r.tokens = $tokens             
                                               """,
                                fromWallet=from_address,
                                toWallet=to_address,
                                borrowLogTimestamps=timestamp,
                                borrowLogValues=value_usd,
                                totalNumberOfBorrow=total_number,
                                totalAmountOfBorrowInUSD=total_amount,
                                highestBorrowInUSD=highest_value,
                                lowestValueBorrowInUSD=lowest_value,
                                averageBorrowInUSD=avg_value,
                                medianBorrowInUSD=median,
                                sortValues=sort_values,
                                borrowTokens=current_tokens,
                                borrowTokenBalances=tokens_amount,
                                tokens=current_tokens
                                ).data()
        return merge

    def create_repay_relationship(self, repay: Repay):
        value_usd = repay.valueUsd
        from_address = repay.fromWallet
        to_address = repay.toAddress
        timestamp = repay.timestamp
        relationship_type = RelationshipType.REPAY
        total_number, total_amount, highest_value, lowest_value, avg_value, median, sort_values, tokens = update_info_merge_relationship(
            self._graph, from_address, to_address, value_usd, relationship_type)

        token = repay.token
        current_tokens = tokens
        wallet_address = from_address
        related_wallets = repay.relatedWallets
        balance_type = WalletConstant.borrow
        current_tokens, tokens_amount = update_token_balance_relationship(token, current_tokens, wallet_address,
                                                                          related_wallets, balance_type)
        """
        update relationship repay
        """

        merge = self._graph.run("""
                                        MATCH (a { address: $fromWallet }), (b {address: $toWallet}) 
                                        MERGE (a)-[r:REPAYS ]->(b)
                                        SET r.repayLogTimestamps = coalesce(r.repayLogTimestamps, []) + $repayLogTimestamp,
                                            r.repayLogValues = coalesce(r.repayLogValues, []) + $repayLogValue,
                                            r.totalNumberOfRepay = $totalNumberOfRepay,
                                            r.totalAmountOfRepayInUSD= $totalAmountOfRepayInUSD,
                                            r.highestValueRepayInUSD = $highestValueRepayInUSD,
                                            r.lowestValueRepayInUSD = $lowestValueRepayInUSD,
                                            r.averageValueRepayInUSD = $averageValueRepayInUSD,
                                            r.medianValueRepayInUSD = $medianValueRepayInUSD,                
                                            r.sortValues = $sortValues              
                                        """,
                                fromWallet=from_address,
                                toWallet=to_address,
                                repayLogTimestamp=timestamp,
                                repayLogValue=value_usd,
                                totalNumberOfRepay=total_number,
                                totalAmountOfRepayInUSD=total_amount,
                                highestValueRepayInUSD=highest_value,
                                lowestValueRepayInUSD=lowest_value,
                                averageValueRepayInUSD=avg_value,
                                medianValueRepayInUSD=median,
                                sortValues=sort_values,
                                ).data()
        """
        update 
        borrowTokens: mảng lưu các token đang được token borrow trong lending pool
        borrowTokenBalances: mảng lưu số lượng của token tương ứng với tokenBorrows đang được wallet borrow 
        cho borrow
        """
        merge = self._graph.run("""
                                                      MATCH (a { address: $fromWallet }), (b {address: $toWallet}) 
                                                      MERGE (a)-[r:BORROWS ]->(b)
                                                      SET r.borrowTokens = $borrowTokens,
                                                          r.borrowTokenBalances = $borrowTokenBalances,
                                                          r.tokens = $tokens             
                                                      """,
                                fromWallet=from_address,
                                toWallet=to_address,
                                borrowTokens=current_tokens,
                                borrowTokenBalances=tokens_amount,
                                tokens=current_tokens
                                ).data()
        return merge

    def create_withdraw_relationship(self, withdraw: Withdraw):
        value_usd = withdraw.valueUsd
        from_address = withdraw.fromWallet
        to_address = withdraw.toAddress
        timestamp = withdraw.timestamp
        relationship_type = RelationshipType.WITHDRAW
        total_number, total_amount, highest_value, lowest_value, avg_value, median, sort_values, tokens = update_info_merge_relationship(
            self._graph, from_address, to_address, value_usd, relationship_type)

        token = withdraw.token
        current_tokens = tokens
        wallet_address = from_address
        related_wallets = withdraw.relatedWallets
        balance_type = WalletConstant.supply
        current_tokens, tokens_amount = update_token_balance_relationship(token, current_tokens, wallet_address,
                                                                          related_wallets, balance_type)
        """
        update relationship Withdraw
        """

        merge = self._graph.run("""
                                                MATCH (a { address: $fromWallet }), (b {address: $toWallet}) 
                                                MERGE (a)-[r:WITHDRAWS ]->(b)
                                                SET r.withdrawLogTimestamps = coalesce(r.withdrawLogTimestamps, []) + $withdrawLogTimestamp,
                                                    r.withdrawLogValues = coalesce(r.withdrawLogValues, []) + $withdrawLogValue,
                                                    r.totalNumberOfWithdraw = $totalNumberOfWithdraw,
                                                    r.totalAmountOfWithdrawInUSD= $totalAmountOfWithdrawInUSD,
                                                    r.highestValueWithdrawInUSD = $highestValueWithdrawInUSD,
                                                    r.lowestValueWithdrawInUSD = $lowestValueWithdrawInUSD,
                                                    r.averageValueWithdrawInUSD = $averageValueWithdrawInUSD,
                                                    r.medianValueWithdrawInUSD = $medianValueWithdrawInUSD,                
                                                    r.sortValues = $sortValues              
                                                """,
                                fromWallet=from_address,
                                toWallet=to_address,
                                withdrawLogTimestamp=timestamp,
                                withdrawLogValue=value_usd,
                                totalNumberOfWithdraw=total_number,
                                totalAmountOfWithdrawInUSD=total_amount,
                                highestValueWithdrawInUSD=highest_value,
                                lowestValueWithdrawInUSD=lowest_value,
                                averageValueWithdrawInUSD=avg_value,
                                medianValueWithdrawInUSD=median,
                                sortValues=sort_values,
                                ).data()
        """
        update 
        depositTokens: mảng lưu các token đang được token deposit trong lending pool
        depositTokenBalances: mảng lưu số lượng của token tương ứng với tokenDeposits 

        cho deposit
        """
        merge = self._graph.run("""
                                                              MATCH (a { address: $fromWallet }), (b {address: $toWallet}) 
                                                              MERGE (a)-[r:DEPOSITS ]->(b)
                                                              SET r.depositTokens = $depositTokens,
                                                                  r.depositTokenBalances = $depositTokenBalances,
                                                                  r.tokens = $tokens             
                                                              """,
                                fromWallet=from_address,
                                toWallet=to_address,
                                depositTokens=current_tokens,
                                depositTokenBalances=tokens_amount,
                                tokens=current_tokens
                                ).data()
        return merge

    def create_liquidate_relationship(self, liquidate: Liquidate):
        merge = self._graph.run("MATCH (a:Wallet { address: $fromWallet }), (b :Wallet {address: $toWallet}) "
                                "MERGE (a)-[r:LIQUIDATE { transactionID: $transactionID,"
                                "timestamp: $timestamp,"
                                "protocol: $protocol,"
                                "fromWallet: $fromWallet,"
                                "toWallet: $toWallet,"
                                "fromBalance: $fromBalance,"
                                "fromAmount: $fromAmount,"
                                "toBalance: $toBalance,"
                                "toAmount: $toAmount }]->(b) RETURN a,b,r",
                                transactionID=liquidate.transactionID,
                                timestamp=liquidate.timestamp,
                                protocol=liquidate.protocol,
                                fromWallet=liquidate.fromWallet,
                                toWallet=liquidate.toWallet,
                                fromBalance=liquidate.fromBalance,
                                fromAmount=liquidate.fromAmount,
                                toBalance=liquidate.toBalance,
                                toAmount=liquidate.toAmount).data()
        return merge
