from py2neo import Graph
from data_aggregation.database.nodes_model import Wallet, Token, LendingPool
from data_aggregation.database.relationships_model import *
from config.config import Neo4jConfig

class CreateGraph:
    def __init__(self):
        self._conn = None

        bolt = f"bolt://{Neo4jConfig.HOST}:{Neo4jConfig.BOTH_PORT}"
        self._graph = Graph(bolt, auth=(Neo4jConfig.NEO4J_USERNAME, Neo4jConfig.NEO4J_PASSWORD))

    # ==================
    # CREATE NODE
    # ==================

    def neo4j_create_wallet_node(self, wallet_address):
        match = self._graph.run("MATCH (p:Wallet { address : $address}) return p ", address=wallet_address).data()
        if not match:
            create = self._graph.run("MERGE (p:Wallet { address: $address }) "
                                "RETURN p",
                                address = wallet_address).data()
            return create[0]["p"]
        return match[0]["p"]

    def neo4j_update_wallet_node(self, wallet: Wallet):
        match = self._graph.run("MATCH (p:Wallet { address : $address}) return p ", address=wallet.address).data()
        if not match:
            create = self._graph.run("MERGE (p:Wallet { address: $address }) "
                                    "SET p.lastUpdatedAt = $lastUpdatedAt,"
                                    "p.creditScore = $creditScore, "
                                    "p.tokens = $tokens, "
                                    "p.tokenBalances = $tokenBalances,"
                                    "p.balanceInUSD = $balanceInUSD, "
                                    "p.balanceChangeLogTimestamps = $balanceChangeLogTimestamps, "
                                    "p.balanceChangeLogValues = $balanceChangeLogValues, "
                                    "p.createdAt = $createdAt, "
                                    "p.depositTokens = $depositTokens, "
                                    "p.depositTokenBalances = $depositTokenBalances, "
                                    "p.depositInUSD = $depositInUSD, "
                                    "p.depositChangeLogTimestamps =$depositChangeLogTimestamps, "
                                    "p.depositChangeLogValues = $depositChangeLogValues, "
                                    "p.borrowTokens = $borrowTokens, "
                                    "p.borrowTokenBalances = $borrowTokenBalances, "
                                    "p.borrowInUSD = $borrowInUSD, "
                                    "p.borrowChangeLogTimestamps = $borrowChangeLogTimestamps, "
                                    "p.borrowChangeLogValues = $borrowChangeLogValues, "
                                    "p.numberOfLiquidation = $numberOfLiquidation, "
                                    "p.totalAmountOfLiquidation = $totalAmountOfLiquidation, "
                                    "p.dailyTransactionAmounts = $dailyTransactionAmounts, "
                                    "p.dailyFrequencyOfTransactions  = $dailyFrequencyOfTransactions "
                                    "RETURN p",
                                    address=wallet.address,
                                    lastUpdatedAt=wallet.lastUpdatedAt,
                                    creditScore=wallet.creditScore,
                                    tokens=wallet.tokens,
                                    tokenBalances=wallet.tokenBalances,
                                    balanceInUSD=wallet.balanceInUSD,
                                    balanceChangeLogTimestamps=wallet.balanceChangeLogTimestamps,
                                    balanceChangeLogValues=wallet.balanceChangeLogValues,
                                    createdAt=wallet.createdAt,
                                    depositTokens=wallet.depositTokens,
                                    depositTokenBalances=wallet.depositTokenBalances,
                                    depositInUSD=wallet.depositInUSD,
                                    depositChangeLogTimestamps=wallet.depositChangeLogTimestamps,
                                    depositChangeLogValues=wallet.depositChangeLogValues,
                                    borrowTokens=wallet.borrowTokens,
                                    borrowTokenBalances=wallet.borrowTokenBalances,
                                    borrowInUSD=wallet.borrowInUSD,
                                    borrowChangeLogTimestamps=wallet.borrowChangeLogTimestamps,
                                    borrowChangeLogValues=wallet.borrowChangeLogValues,
                                    numberOfLiquidation=wallet.numberOfLiquidation,
                                    totalAmountOfLiquidation=wallet.totalAmountOfLiquidation,
                                    dailyTransactionAmounts=wallet.dailyTransactionAmounts,
                                    dailyFrequencyOfTransactions=wallet.dailyFrequencyOfTransactions).data()
        else:
            create = self._graph.run("MATCH (p:Wallet { address: $address }) "
                                    "SET p.lastUpdatedAt = $lastUpdatedAt,"
                                    "p.creditScore = $creditScore, "
                                    "p.tokens = $tokens, "
                                    "p.tokenBalances = $tokenBalances,"
                                    "p.balanceInUSD = $balanceInUSD, "
                                    "p.balanceChangeLogTimestamps = $balanceChangeLogTimestamps, "
                                    "p.balanceChangeLogValues = $balanceChangeLogValues, "
                                    "p.createdAt = $createdAt, "
                                    "p.depositTokens = $depositTokens, "
                                    "p.depositTokenBalances = $depositTokenBalances, "
                                    "p.depositInUSD = $depositInUSD, "
                                    "p.depositChangeLogTimestamps =$depositChangeLogTimestamps, "
                                    "p.depositChangeLogValues = $depositChangeLogValues, "
                                    "p.borrowTokens = $borrowTokens, "
                                    "p.borrowTokenBalances = $borrowTokenBalances, "
                                    "p.borrowInUSD = $borrowInUSD, "
                                    "p.borrowChangeLogTimestamps = $borrowChangeLogTimestamps, "
                                    "p.borrowChangeLogValues = $borrowChangeLogValues, "
                                    "p.numberOfLiquidation = $numberOfLiquidation, "
                                    "p.totalAmountOfLiquidation = $totalAmountOfLiquidation, "
                                    "p.dailyTransactionAmounts = $dailyTransactionAmounts, "
                                    "p.dailyFrequencyOfTransactions  = $dailyFrequencyOfTransactions "
                                    "RETURN p",
                                    address=wallet.address,
                                    lastUpdatedAt=wallet.lastUpdatedAt,
                                    creditScore=wallet.creditScore,
                                    tokens=wallet.tokens,
                                    tokenBalances=wallet.tokenBalances,
                                    balanceInUSD=wallet.balanceInUSD,
                                    balanceChangeLogTimestamps=wallet.balanceChangeLogTimestamps,
                                    balanceChangeLogValues=wallet.balanceChangeLogValues,
                                    createdAt=wallet.createdAt,
                                    depositTokens=wallet.depositTokens,
                                    depositTokenBalances=wallet.depositTokenBalances,
                                    depositInUSD=wallet.depositInUSD,
                                    depositChangeLogTimestamps=wallet.depositChangeLogTimestamps,
                                    depositChangeLogValues=wallet.depositChangeLogValues,
                                    borrowTokens=wallet.borrowTokens,
                                    borrowTokenBalances=wallet.borrowTokenBalances,
                                    borrowInUSD=wallet.borrowInUSD,
                                    borrowChangeLogTimestamps=wallet.borrowChangeLogTimestamps,
                                    borrowChangeLogValues=wallet.borrowChangeLogValues,
                                    numberOfLiquidation=wallet.numberOfLiquidation,
                                    totalAmountOfLiquidation=wallet.totalAmountOfLiquidation,
                                    dailyTransactionAmounts=wallet.dailyTransactionAmounts,
                                    dailyFrequencyOfTransactions=wallet.dailyFrequencyOfTransactions).data()
        return create[0]["p"]

    def neo4j_create_token_node(self, token_address):
        match = self._graph.run("MATCH (p:Token { address : $address}) return p ", address=token_address).data()
        if not match:
            create = self._graph.run("MERGE (p:Token { address: $address }) "
                                "RETURN p",
                                address = token_address).data()
            return create[0]["p"]
        return match[0]["p"]

    def neo4j_update_token_node(self, token: Token):
        match = self._graph.run("MATCH (a:Token { address: $address }) RETURN a", address=token.address).data()
        if not match:
            create = self._graph.run("MERGE (a: Token { address: $address })"
                                    "SET a.totalSupply = $totalSupply,"
                                    "a.symbol = $symbol,"
                                    "a.name = $name,"
                                    "a.decimal = $decimal,"
                                    "a.dailyFrequencyOfTransactions = $dailyFrequencyOfTransactions, "
                                    "a.creditScore = $creditScore, "
                                    "a.price = $price,"
                                    "a.highestPrice = $highestPrice, "
                                    "a.marketCap = $marketCap,"
                                    "a.tradingVolume24 = $tradingVolume24, "
                                    "a.lastUpdatedAt = $lastUpdatedAt "
                                    "RETURN a",
                                    address=token.address,
                                    totalSupply=token.totalSupply,
                                    symbol=token.symbol,
                                    name=token.name,
                                    decimal=token.decimal,
                                    dailyFrequencyOfTransactions=token.dailyFrequencyOfTransactions,
                                    creditScore=token.creditScore,
                                    price=token.price,
                                    highestPrice=token.highestPrice,
                                    marketCap=token.marketCap,
                                    tradingVolume24=token.tradingVolume24,
                                    lastUpdatedAt=token.lastUpdatedAt).data()
        else:
            create = self._graph.run("MATCH (a: Token { address: $address })"
                                    "SET a.totalSupply = $totalSupply,"
                                    "a.symbol = $symbol,"
                                    "a.name = $name,"
                                    "a.decimal = $decimal,"
                                    "a.dailyFrequencyOfTransactions = $dailyFrequencyOfTransactions, "
                                    "a.creditScore = $creditScore, "
                                    "a.price = $price,"
                                    "a.highestPrice = $highestPrice, "
                                    "a.marketCap = $marketCap,"
                                    "a.tradingVolume24 = $tradingVolume24, "
                                    "a.lastUpdatedAt = $lastUpdatedAt "
                                    "RETURN a",
                                    address=token.address,
                                    totalSupply=token.totalSupply,
                                    symbol=token.symbol,
                                    name=token.name,
                                    decimal=token.decimal,
                                    dailyFrequencyOfTransactions=token.dailyFrequencyOfTransactions,
                                    creditScore=token.creditScore,
                                    price=token.price,
                                    highestPrice=token.highestPrice,
                                    marketCap=token.marketCap,
                                    tradingVolume24=token.tradingVolume24,
                                    lastUpdatedAt=token.lastUpdatedAt).data()
            return create[0]['a']

    def neo4j_create_lending_pool_node(self, lending_pool_address):
        match = self._graph.run("MATCH (p:LendingPool { address : $address}) return p ", address=lending_pool_address).data()
        if not match:
            create = self._graph.run("MERGE (p:LendingPool { address: $address }) "
                                "RETURN p",
                                address = lending_pool_address).data()
            return create[0]["p"]
        return match[0]["p"]

    def neo4j_update_lending_pool_node(self, lendingPool: LendingPool):
        match = self._graph.run("MATCH (a:LendingPool { address: $address }) RETURN a", address=lendingPool.address).data()
        if not match:
            create = self._graph.run("MERGE (a:LendingPool { address: $address })"
                                    "SET a.tokens=$tokens,"
                                    "a.supply=$supply,"
                                    "a.borrow=$borrow "
                                    "RETURN a",
                                    address=lendingPool.address,
                                    tokens=lendingPool.tokens,
                                    supply=lendingPool.supply,
                                    borrow=lendingPool.borrow).data()
        else:
            create = self._graph.run("MATCH (a:LendingPool { address: $address })"
                                    "SET a.tokens=$tokens,"
                                    "a.supply=$supply,"
                                    "a.borrow=$borrow "
                                    "RETURN a",
                                    address=lendingPool.address,
                                    tokens=lendingPool.tokens,
                                    supply=lendingPool.supply,
                                    borrow=lendingPool.borrow).data()
        return create[0]['a']

    def neo4j_update_user_node(self):
        pass

    # ==================
    # CREATE RELATIONSHIP
    # ==================

    def neo4j_update_transfer_relationship(self, transfer: Transfer):
        merge = self._graph.run("MATCH (a:Wallet { address: $fromWallet }), (b:Wallet {address: $toWallet}) "
                                "MERGE (a)-[r:TRANSFER { transactionID: $transactionID,"
                                "timestamp: $timestamp,"
                                "fromWallet: $fromWallet,"
                                "toWallet: $toWallet,"
                                "token: $token,"
                                "value: $value }]->(b) RETURN a,b,r",
                                transactionID=transfer.transactionID,
                                timestamp=transfer.timestamp,
                                fromWallet=transfer.fromWallet,
                                toWallet=transfer.toWallet,
                                token=transfer.token,
                                value=transfer.value).data()
        return merge

    def neo4j_update_deposit_relationship(self, deposit: Deposit):
        merge = self._graph.run("MATCH (a:Wallet { address: $fromWallet }), (b:Token { address: $toAddress}) "
                                "MERGE (a)-[r:DEPOSIT { transactionID: $transactionID,"
                                "timestamp: $timestamp,"
                                "fromWallet: $fromWallet,"
                                "toAddress: $toAddress,"
                                "token: $token,"
                                "value: $value }]->(b) RETURN a,b,r",
                                transactionID=deposit.transactionID,
                                timestamp=deposit.timestamp,
                                fromWallet=deposit.fromWallet,
                                toAddress=deposit.toAddress,
                                token=deposit.token,
                                value=deposit.value).data()
        return merge

    def neo4j_update_borrow_relationship(self, borrow: Borrow):
        merge = self._graph.run("MATCH (a:Wallet { address: $fromWallet }), (b:Token {address: $toAddress}) "
                                "MERGE (a)-[r:BORROW { transactionID: $transactionID,"
                                "timestamp: $timestamp,"
                                "fromWallet: $fromWallet,"
                                "toAddress: $toAddress,"
                                "token: $token,"
                                "value: $value }]->(b) RETURN a,b,r",
                                transactionID=borrow.transactionID,
                                timestamp=borrow.timestamp,
                                fromWallet=borrow.fromWallet,
                                toAddress=borrow.toAddress,
                                token=borrow.token,
                                value=borrow.value).data()
        return merge

    def neo4j_update_repay_relationship(self, repay: Repay):
        merge = self._graph.run("MATCH (a:Wallet { address: $fromWallet }), (b:Token {address: $toAddress}) "
                                "MERGE (a)-[r:REPAY { transactionID: $transactionID,"
                                "timestamp: $timestamp,"
                                "fromWallet: $fromWallet,"
                                "toAddress: $toAddress,"
                                "token: $token,"
                                "value: $value }]->(b) RETURN a,b,r",
                                transactionID=repay.transactionID,
                                timestamp=repay.timestamp,
                                fromWallet=repay.fromWallet,
                                toAddress=repay.toAddress,
                                token=repay.token,
                                value=repay.value).data()
        return merge

    def neo4j_update_withdraw_relationship(self, withdraw: Withdraw):
        merge = self._graph.run("MATCH (a:Wallet { address: $fromWallet }), (b:Token {address: $toAddress}) "
                                "MERGE (a)-[r:WITHDRAW { transactionID: $transactionID,"
                                "timestamp: $timestamp,"
                                "fromWallet: $fromWallet,"
                                "toAddress: $toAddress,"
                                "token: $token,"
                                "value: $value }]->(b) RETURN a,b,r",
                                transactionID=withdraw.transactionID,
                                timestamp=withdraw.timestamp,
                                fromWallet=withdraw.fromWallet,
                                toAddress=withdraw.toAddress,
                                token=withdraw.token,
                                value=withdraw.value).data()
        return merge

    def neo4j_update_liquidate_relationship(self, liquidate: Liquidate):
        merge = self._graph.run("MATCH (a:Wallet { address: $fromWallet }), (b:Wallet {address: $toWallet}) "
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

    def neo4j_update_hold_relationship(self):
        pass