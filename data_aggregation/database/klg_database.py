from py2neo import Graph

from config.config import Neo4jConfig


class KlgDatabase(object):
    """Manages connection to  database_common and makes async queries
    """

    def __init__(self):
        self._conn = None

        bolt = f"bolt://{Neo4jConfig.HOST}:{Neo4jConfig.BOTH_PORT}"
        self._graph = Graph(bolt, auth=(Neo4jConfig.NEO4J_USERNAME, Neo4jConfig.NEO4J_PASSWORD))

    def update_wallet_token(self, token_map={}, balance=0):
        """
        cập nhật  trường
        Token:[map trong neo4j] ghi lại các loại token và số lượng từng loại có trong wallet.
        Key: token address - value: số lượng token mà wallet đang giữ
        Balance:Giá trị tài khoản = Σ(value_token*token_price)(USD)
        :return:
        """
        pass

    def update_wallet_token_deposit_and_borrow(self, token_deposit_map={}, token_borrow_map={}, deposit=0, borrow=0):
        """
        cập nhật các trường

        Token_Deposit: [map trong neo4j] ghi lại các loại token và số lượng từng loại mà wallet deposit.
        Key: token address - value: số lượng token mà wallet đang deposit

        &&
        Token_Borrow: [map trong neo4j] ghi lại các loại token và số lượng từng loại mà wallet borrow.
        Key: token address - value: số lượng token mà wallet đang borrow

        &&
        Deposit(USD): lượng tiền đang deposit ở các lending pool - tính ra USD

        $$
        Borrow: lượng tiền đang đi vay ở các lending pool -(USD)
        :return:
        """
        pass

    def update_wallet_created_at(self, wallet_address, created_at):
        """
        cập nhật các trường

        Created at: timestamp mà wallet thực hiện giao dịch đầu tiên trên bsc
        :return:
        """
        pass
