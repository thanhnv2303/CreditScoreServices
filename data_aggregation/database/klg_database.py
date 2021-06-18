from py2neo import Graph

from config.config import Neo4jConfig
from services.zip_service import two_list_to_dict, dict_to_two_list


class KlgDatabase(object):
    """Manages connection to  database_common and makes async queries
    """

    def __init__(self):
        self._conn = None

        bolt = f"bolt://{Neo4jConfig.HOST}:{Neo4jConfig.BOTH_PORT}"
        self._graph = Graph(bolt, auth=(Neo4jConfig.NEO4J_USERNAME, Neo4jConfig.NEO4J_PASSWORD))

    def update_wallet_token(self,wallet_address, token_map={}, balance=0):
        # """
        # cập nhật  trường
        # Token:[map trong neo4j] ghi lại các loại token và số lượng từng loại có trong wallet.
        # Key: token address - value: số lượng token mà wallet đang giữ
        # Balance:Giá trị tài khoản = Σ(value_token*token_price)(USD)
        # :return:
        # """

        tokens, tokenBalances = dict_to_two_list(token_map)
        match = self._graph.run("MATCH (p:Wallet { address : $address}) return p ", address=wallet_address).data()
        if not match:
            create = self._graph.run("MERGE (p:Wallet { address: $address }) "
                                        "SET p.tokens = $tokens, "
                                        "p.tokenBalances = $tokenBalances, "
                                        "p.balanceInUSD = $balanceInUSD "
                                        "RETURN p",
                                        address = wallet_address, 
                                        tokens = tokens,
                                        tokenBalances = tokenBalances,
                                        balanceInUSD = balance).data()
        else:
            create = self._graph.run("MATCH (p:Wallet { address: $address }) "
                                        "SET p.tokens = $tokens, "
                                        "p.tokenBalances = $tokenBalances, "
                                        "p.balanceInUSD = $balanceInUSD "
                                        "RETURN p",
                                        address = wallet_address, 
                                        tokens = tokens,
                                        tokenBalances = tokenBalances,
                                        balanceInUSD = balance).data()
            
        return create[0]["p"]

    def update_wallet_token_deposit_and_borrow(self,wallet_address, token_deposit_map={}, token_borrow_map={}, deposit=0, borrow=0):
        # """
        # cập nhật các trường

        # Token_Deposit: [map trong neo4j] ghi lại các loại token và số lượng từng loại mà wallet deposit.
        # Key: token address - value: số lượng token mà wallet đang deposit

        # &&
        # Token_Borrow: [map trong neo4j] ghi lại các loại token và số lượng từng loại mà wallet borrow.
        # Key: token address - value: số lượng token mà wallet đang borrow

        # &&
        # Deposit(USD): lượng tiền đang deposit ở các lending pool - tính ra USD

        # $$
        # Borrow: lượng tiền đang đi vay ở các lending pool -(USD)
        # :return:
        # """
        depositTokens, depositTokenBalances = dict_to_two_list(token_deposit_map)
        borrowTokens, borrowTokenBalances = dict_to_two_list(token_borrow_map)

        match = self._graph.run("MATCH (p:Wallet { address : $address}) return p ", address=wallet_address).data()
        if not match:
            create = self._graph.run("MERGE (p:Wallet { address: $address }) "
                                        "SET p.depositTokens = $depositTokens, "
                                        "p.depositTokenBalances = $depositTokenBalances, "
                                        "p.borrowTokens = $borrowTokens, "
                                        "p.borrowTokenBalances = $borrowTokenBalances, "
                                        "p.depositInUSD = $depositInUSD, "
                                        "p.borrowInUSD = $borrowInUSD "
                                        "RETURN p",
                                        address = wallet_address, 
                                        depositTokens = depositTokens,
                                        depositTokenBalances = depositTokenBalances,
                                        borrowTokens = borrowTokens,
                                        borrowTokenBalances = borrowTokenBalances,
                                        depositInUSD = deposit,
                                        borrowInUSD = borrow).data()
        else:
            create = self._graph.run("MATCH (p:Wallet { address: $address }) "
                                        "SET p.depositTokens = $depositTokens, "
                                        "p.depositTokenBalances = $depositTokenBalances, "
                                        "p.borrowTokens = $borrowTokens, "
                                        "p.borrowTokenBalances = $borrowTokenBalances, "
                                        "p.depositInUSD = $depositInUSD, "
                                        "p.borrowInUSD = $borrowInUSD "
                                        "RETURN p",
                                        address = wallet_address, 
                                        depositTokens = depositTokens,
                                        depositTokenBalances = depositTokenBalances,
                                        borrowTokens = borrowTokens,
                                        borrowTokenBalances = borrowTokenBalances,
                                        depositInUSD = deposit,
                                        borrowInUSD = borrow).data()
            
        return create[0]["p"]


    def update_wallet_created_at(self, wallet_address, created_at):
        # """
        # cập nhật các trường

        # Created at: timestamp mà wallet thực hiện giao dịch đầu tiên trên bsc
        # :return:
        # """
        # pass
        match = self._graph.run("MATCH (p:Wallet { address : $address}) return p ", address=wallet_address).data()
        if not match:
            create = self._graph.run("MERGE (p:Wallet { address: $address }) "
                                        "SET p.createdAt = $createdAt "
                                        "RETURN p",
                                        address = wallet_address, createdAt = created_at).data()
        else:
            create = self._graph.run("MATCH (p:Wallet { address: $address }) "
                                        "SET p.createdAt = $createdAt "
                                        "RETURN p",
                                        address = wallet_address, createdAt = created_at).data()
            
        return create[0]["p"]



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

        getter = self._graph.run("match (p:Wallet { address: $address }) return p.balanceChangeLogTimestamps ", address = wallet_address).data()
        keys_list = keys_list + getter[0]["p.balanceChangeLogTimestamps"]

        getter = self._graph.run("match (p:Wallet { address: $address }) return p.balanceChangeLogValues ", address = wallet_address).data()
        values_list = values_list + getter[0]["p.balanceChangeLogValues"]

        return two_list_to_dict(keys_list, values_list)

    def update_balance_100(self, wallet_address, balance_100):
        # """
        # cập nhật
        # balance100 : [map ] ghi lại các lần balance thay đổi trong 100 ngày qua -
        # key: timestamp(các timestamp thay đổi balance) - value: giá trị tại balance timestamp đó

        # :param wallet_address:
        # :return:
        # """
        keys, values = dict_to_two_list(balance_100)

        match = self._graph.run("MATCH (p:Wallet { address : $address}) return p ", address=wallet_address).data()
        if not match:
            create = self._graph.run("MERGE (p:Wallet { address: $address }) "
                                        "SET p.balanceChangeLogTimestamps = $balance100, "
                                        "p.balanceChangeLogValues = $balance100value "
                                        "RETURN p",
                                        address = wallet_address, balance100 = keys, balance100value = values).data()
        else:
            create = self._graph.run("MATCH (p:Wallet { address: $address }) "
                                        "SET p.balanceChangeLogTimestamps = $balance100, "
                                        "p.balanceChangeLogValues = $balance100value "
                                        "RETURN p",
                                        address = wallet_address, balance100 = keys, balance100value = values).data()
            
        return create[0]["p"]

    def get_deposit_100(self, wallet_address):
        # """
        # Lấy ra
        # deposit100: [map trong neo4j] ghi lại các lần lượng deposit thay đổi trong 100 ngày qua -
        # key: timestamp(các timestamp thay đổi deposit) - value: giá trị deposit tại timestamp đó

        # :param wallet_address:
        # :return:
        # """
        keys_list = []  # timestamp_deposit_100 list
        values_list = []  # deposit_100 list

        getter = self._graph.run("match (p:Wallet { address: $address }) return p.depositChangeLogTimestamps ", address = wallet_address).data()
        keys_list = keys_list + getter[0]["p.depositChangeLogTimestamps"]

        getter = self._graph.run("match (p:Wallet { address: $address }) return p.depositChangeLogValues ", address = wallet_address).data()
        values_list = values_list + getter[0]["p.depositChangeLogValues"]

        return two_list_to_dict(keys_list, values_list)

    def update_deposit_100(self, wallet_address, deposit_100):
        # """
        # cập nhật
        # deposit100: [map trong neo4j] ghi lại các lần lượng deposit thay đổi trong 100 ngày qua -
        # key: timestamp(các timestamp thay đổi deposit) - value: giá trị deposit tại timestamp đó

        # :param wallet_address:
        # :return:
        # """
        keys, values = dict_to_two_list(deposit_100)

        match = self._graph.run("MATCH (p:Wallet { address : $address}) return p ", address=wallet_address).data()
        if not match:
            create = self._graph.run("MERGE (p:Wallet { address: $address }) "
                                        "SET p.depositChangeLogTimestamps = $deposit100, "
                                        "p.depositChangeLogValues = $deposit100value "
                                        "RETURN p",
                                        address = wallet_address, deposit100 = keys, deposit100value = values).data()
        else:
            create = self._graph.run("MATCH (p:Wallet { address: $address }) "
                                        "SET p.depositChangeLogTimestamps = $deposit100, "
                                        "p.depositChangeLogValues = $deposit100value "
                                        "RETURN p",
                                        address = wallet_address, deposit100 = keys, deposit100value = values).data()
            
        return create[0]["p"]

    def get_borrow_100(self, wallet_address):
        # """
        # Lấy ra
        # balance100 : [map trong neo4j] ghi lại các lần balance thay đổi trong 100 ngày qua -
        # key: timestamp(các timestamp thay đổi balance) - value: giá trị tại balance timestamp đó

        # :param wallet_address:
        # :return:
        # """
        keys_list = []  # timestamp_borrow_100 list
        values_list = []  # borrow_100 list

        getter = self._graph.run("match (p:Wallet { address: $address }) return p.borrowChangeLogTimestamps ", address = wallet_address).data()
        keys_list = keys_list + getter[0]["p.borrowChangeLogTimestamps"]

        getter = self._graph.run("match (p:Wallet { address: $address }) return p.borrowChangeLogValues ", address = wallet_address).data()
        values_list = values_list + getter[0]["p.borrowChangeLogValues"]

        return two_list_to_dict(keys_list, values_list)

    def update_borrow_100(self, wallet_address, borrow_100):
        # """
        # cập nhật
        # borrow100: [map trong neo4j] ghi lại các lần lượng borrow thay đổi trong 100 ngày qua -
        # key: timestamp(các timestamp thay đổi borrow) - value: giá trị borrow tại timestamp đó


        # :param wallet_address:
        # :return:
        # """
        keys, values = dict_to_two_list(borrow_100)

        match = self._graph.run("MATCH (p:Wallet { address : $address}) return p ", address=wallet_address).data()
        if not match:
            create = self._graph.run("MERGE (p:Wallet { address: $address }) "
                                        "SET p.borrowChangeLogTimestamps = $borrow100, "
                                        "p.borrowChangeLogValues = $borrow100value "
                                        "RETURN p",
                                        address = wallet_address, borrow100 = keys, borrow100value = values).data()
        else:
            create = self._graph.run("MATCH (p:Wallet { address: $address }) "
                                        "SET p.borrowChangeLogTimestamps = $borrow100, "
                                        "p.borrowChangeLogValues = $borrow100value "
                                        "RETURN p",
                                        address = wallet_address, borrow100 = keys, borrow100value = values).data()
            
        return create[0]["p"]
