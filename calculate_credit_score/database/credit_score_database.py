from py2neo import Graph

from config.config import Neo4jConfig
from config.constant import LendingPoolConstant, WalletConstant, RelationshipConstant


class Database(object):
    """Manages connection to  database_common and makes async queries
    """

    def __init__(self):
        self._conn = None
        """
        connect database_common neo4j
        """
        bolt = f"bolt://{Neo4jConfig.HOST}:{Neo4jConfig.BOTH_PORT}"
        self._graph = Graph(bolt, auth=(Neo4jConfig.NEO4J_USERNAME, Neo4jConfig.NEO4J_PASSWORD))

    def get_wallets_paging(self, skip, limit):
        """
        trả về một mảng các ví để có thể xử lý
        :rtype: object
        """
        ##fake data
        return []

    def update_wallet(self, wallet):
        pass

    def get_wallet(self, address):
        return {}

    def update_wallet_credit(self, wallet):
        pass

    def get_statistic_credit(self, checkpoint):
        return {}

    def update_statistic_credit(self, statistic_credit):
        pass

    def push_tolist_statistic_credit(self, checkpoint, list_name, element):
        pass

    def delete_statistic_credit(self, checkpoint):
        pass

    def insert_to_token_collection(self, token_address, event):
        pass

    def update_token(self, token):
        pass

    def get_token(self, address):
        pass

    def get_event_at_block_num(self, contract_address, block_num):
        pass

    def get_num_transaction_at_contract(self, contract_address, gt_block_number=0):
        return 0

    def neo4j_update_credit_score(self, address, credit_score):
        pass

    def neo4j_update_wallet_token(self, wallet, token):
        pass

    def neo4j_update_lending_token(self, lending_pool, token):
        pass

    def neo4j_update_token(self, token):
        pass

    def neo4j_update_link(self, tx):
        pass

    def generate_lending_pool_dict_for_klg(self, address, name, borrow, supply, block_number):
        return {
            LendingPoolConstant.address: address,
            LendingPoolConstant.name: name,
            LendingPoolConstant.total_borrow: borrow,
            LendingPoolConstant.total_supply: supply,
            LendingPoolConstant.block_number: block_number
        }

    def generate_wallet_dict_for_klg(self, address, balance, borrow=None, supply=None, credit_score=None,
                                     block_number=None):
        return {
            WalletConstant.address: address,
            WalletConstant.balance: balance,
            WalletConstant.supply: supply,
            WalletConstant.borrow: borrow,
            WalletConstant.credit_score: credit_score,
            WalletConstant.at_block_number: block_number
        }

    def generate_link_dict_for_klg(self, from_address, to_address, tx_id, amount, token, label):
        return {
            RelationshipConstant.from_address: from_address,
            RelationshipConstant.to_address: to_address,
            RelationshipConstant.tx_id: tx_id,
            RelationshipConstant.amount: amount,
            RelationshipConstant.token: token,
            RelationshipConstant.label: label
        }
