import time

from config.performance_constant import PerformanceConstant
from database_common.memory_storage_test_performance import MemoryStoragePerformance

performance_storage = MemoryStoragePerformance.getInstance()


class RelationshipType:
    TRANSFER = "Transfer"
    DEPOSIT = "Deposit"
    BORROW = "Borrow"
    WITHDRAW = "Withdraw"
    REPAY = "Repay"


def get_info_relationship(graph, from_address, to_address, relationship_type=RelationshipType.TRANSFER):
    cypher = f"""
            MATCH (a {{ address: "{from_address}" }})-[r:{relationship_type.upper()}S]-> (b {{address: "{to_address}" }})
            RETURN  r.totalNumberOf{relationship_type}, r.totalAmountOf{relationship_type}InUSD,
                    r.highestValue{relationship_type}InUSD, r.lowestValue{relationship_type}InUSD,
                    r.sortValues, r.tokens
    """
    total_number = 0
    total_amount = 0
    highest_value = 0
    lowest_value = 0
    sort_values = []
    tokens = []

    start = time.time()
    getter = graph.run(cypher).data()
    duration = time.time() - start
    performance_storage.accumulate_to_key(PerformanceConstant.create_withdraw_relationship, duration)

    totalAmountOf = f"r.totalAmountOf{relationship_type}InUSD"
    totalNumberOf = f"r.totalNumberOf{relationship_type}"
    highestValue = f"r.highestValue{relationship_type}InUSD"
    lowestValue = f"r.lowestValue{relationship_type}InUSD"
    if getter and getter[0] and getter[0].get(totalNumberOf):
        total_number = getter[0].get(totalNumberOf) or 0
        total_amount = getter[0].get(totalAmountOf) or 0
        highest_value = getter[0].get(highestValue) or 0
        lowest_value = getter[0].get(lowestValue) or 0
        sort_values = getter[0].get("r.sortValues") or []
        tokens = getter[0].get("r.tokens") or []

    return total_number, total_amount, highest_value, lowest_value, sort_values, tokens
