from config.constant import WalletConstant
from data_aggregation.database.neo4j_services_db.cypher_transfer import get_info_relationship, RelationshipType


class BalanceType:
    BALANCE = "balance"
    DEPOSIT = "supply"
    BORROW = "borrow"


def update_info_merge_relationship(graph, from_address, to_address, value_usd,
                                   relationship_type=RelationshipType.TRANSFER):
    """

    :param graph:
    :param from_address:
    :param to_address:
    :param value_usd:
    :param relationship_type:
    :return:
    """
    total_number, total_amount, highest_value, lowest_value, sort_values, tokens = get_info_relationship(graph,
                                                                                                         from_address,
                                                                                                         to_address,
                                                                                                         relationship_type)
    total_number += 1
    total_amount += value_usd
    highest_value = max(highest_value, value_usd)
    lowest_value = min(lowest_value, value_usd)
    avg_value = total_amount / total_number
    median = 0
    i = 0
    while i < len(sort_values):
        if sort_values[i] > value_usd:
            break
        i += 1
    sort_values.insert(i, value_usd)

    if total_number % 2:
        i = total_number / 2
        median = (sort_values[i] + sort_values[i - 1]) / 2

    return total_number, total_amount, highest_value,lowest_value, avg_value, median, sort_values, tokens


def update_token_balance_relationship(token="", current_tokens=[], wallet_address="", related_wallets=[],
                                      balance_type=WalletConstant.supply):
    if token not in current_tokens:
        current_tokens.append(token)
    update_wallet = {}
    for wallet in related_wallets:
        if wallet.get(WalletConstant.address) == wallet_address:
            update_wallet = wallet
    balance_at_type = update_wallet.get(balance_type)
    tokens_amount = []
    for token in current_tokens:
        tokens_amount.append(balance_at_type.get(token))

    return current_tokens, tokens_amount
