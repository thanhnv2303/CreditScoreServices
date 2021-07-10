from data_aggregation.database.create_graph import CreateGraph
from data_aggregation.services.mainnet_bsc_address import TOKENS, VTOKENS, TRAVA_POOLS
from data_aggregation.services.mainnet_ether_address import ETH_TOKENS, ETH_POOLS
from data_aggregation.services.testnet_bsc_address import TEST_NET_BNB_TOKENS, TEST_NET_BNB_VTOKENS, \
    TEST_NET_BNB_TRAVA_POOLS


def init_graph_mainnet():
    create_graph = CreateGraph()

    for token in TOKENS:
        create_graph.neo4j_create_token_node(token)

    for vtoken in VTOKENS:
        create_graph.neo4j_create_lending_pool_node(vtoken)
    for trava_pool in TRAVA_POOLS:
        create_graph.neo4j_create_lending_pool_node(trava_pool)


def init_graph_testnet():
    create_graph = CreateGraph()

    for token in TEST_NET_BNB_TOKENS:
        create_graph.neo4j_create_token_node(token)

    for vtoken in TEST_NET_BNB_VTOKENS:
        create_graph.neo4j_create_lending_pool_node(vtoken)
    for trava_pool in TEST_NET_BNB_TRAVA_POOLS:
        create_graph.neo4j_create_lending_pool_node(trava_pool)

def init_graph_ether():
    create_graph = CreateGraph()

    for token in ETH_TOKENS:
        create_graph.neo4j_create_token_node(token)

    for pool in ETH_POOLS:
        create_graph.neo4j_create_lending_pool_node(pool)