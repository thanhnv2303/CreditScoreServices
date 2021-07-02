class RelationshipType:
    TRANSFER = "Transfer"
    DEPOSIT = "Deposit"
    BORROW = "Borrow"
    WITHDRAW = "Withdraw"
    REPAY = "Repay"


def get_transfer_info(graph, from_address, to_address):
    cypher = f"""
            MATCH (a {{ address: {from_address} }})-[r:TRANSFER]-> (b {{address: {to_address}}})
            RETURN  r.totalNumberOfTransfer, r.totalAmountOfTransferInUSD,
                    r.highestValueTransferInUSD, r.sortValues
    """
    total_number = 0
    total_amount = 0
    highest_value = 0
    sort_values = []
    getter = graph.run(cypher)
    if getter and getter[0]["r.totalAmountOfTransferInUSD"]:
        total_number = getter[0]["r.totalNumberOfTransfer"]
        total_amount = getter[0]["r.totalAmountOfTransferInUSD"]
        highest_value = getter[0]["r.highestValueTransferInUSD"]
        sort_values = getter[0]["r.sortValues"]

    return total_number, total_amount, highest_value, sort_values


def get_info_relationship(graph, from_address, to_address, relationship_type=RelationshipType.TRANSFER):
    cypher = f"""
            MATCH (a {{ address: {from_address} }})-[r:{relationship_type.upper()}S]-> (b {{address: {to_address}}})
            RETURN  r.totalNumberOf{relationship_type}, r.totalAmountOf{relationship_type}InUSD,
                    r.highestValue{relationship_type}InUSD, r.sortValues, r.tokens
    """
    total_number = 0
    total_amount = 0
    highest_value = 0
    sort_values = []
    tokens = []
    getter = graph.run(cypher)
    if getter and getter[0][f"r.totalAmountOf{relationship_type}InUSD"]:
        total_number = getter[0][f"r.totalNumberOf{relationship_type}"] or 0
        total_amount = getter[0][f"r.totalAmountOf{relationship_type}InUSD"] or 0
        highest_value = getter[0][f"r.highestValue{relationship_type}InUSD"] or 0
        sort_values = getter[0]["r.sortValues"] or []
        tokens = getter[0]["r.tokens"] or []

    return total_number, total_amount, highest_value, sort_values, tokens
