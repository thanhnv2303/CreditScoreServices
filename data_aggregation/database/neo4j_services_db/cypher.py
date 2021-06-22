INDEX_ADDRESS_WALLET = """
CREATE INDEX address_wallet 
FOR (n:Wallet)
ON (n.address)
"""

INDEX_ADDRESS_TOKEN = """
CREATE INDEX address_token 
FOR (n:Token)
ON (n.address)
"""

INDEX_ADDRESS_LENDING_POOL = """
CREATE INDEX address_lending_pool 
FOR (n:LendingPool)
ON (n.address)
"""
