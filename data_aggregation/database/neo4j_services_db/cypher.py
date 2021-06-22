INDEX_ADDRESS_WALLET = """
CREATE [BTREE] INDEX [address_wallet] [IF NOT EXISTS]
FOR (n:Wallet)
ON (n.address)
"""

INDEX_ADDRESS_TOKEN = """
CREATE [BTREE] INDEX [address_token] [IF NOT EXISTS]
FOR (n:Token)
ON (n.address)
"""

INDEX_ADDRESS_LENDING_POOL = """
CREATE [BTREE] INDEX [address_lending_pool] [IF NOT EXISTS]
FOR (n:LendingPool)
ON (n.address)
"""
