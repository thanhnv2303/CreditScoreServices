class Wallet:
    def __init__(self, address, lastUpdatedAt, creditScore, tokens, tokenBalances, balanceInUSD, balanceChangeLogTimestamps,
                                balanceChangeLogValues, createdAt, depositTokens, depositTokenBalances, depositInUSD, depositChangeLogTimestamps,
                                depositChangeLogValues, borrowTokens, borrowTokenBalances,
                                borrowInUSD, borrowChangeLogTimestamps, borrowChangeLogValues, numberOfLiquidation, totalAmountOfLiquidation,
                                dailyTransactionAmounts, dailyFrequencyOfTransactions ):
        self.address = address
        self.lastUpdatedAt = lastUpdatedAt
        self.creditScore = creditScore
        self.tokens = tokens
        self.tokenBalances = tokenBalances
        self.balanceInUSD = balanceInUSD
        self.balanceChangeLogTimestamps = balanceChangeLogTimestamps
        self.balanceChangeLogValues = balanceChangeLogValues
        self.createdAt = createdAt
        self.depositTokens = depositTokens
        self.depositTokenBalances = depositTokenBalances
        self.depositInUSD = depositInUSD
        self.depositChangeLogTimestamps = depositChangeLogTimestamps
        self.depositChangeLogValues = depositChangeLogValues
        self.borrowTokens = borrowTokens
        self.borrowTokenBalances = borrowTokenBalances
        self.borrowInUSD = borrowInUSD
        self.borrowChangeLogTimestamps = borrowChangeLogTimestamps
        self.borrowChangeLogValues = borrowChangeLogValues
        self.numberOfLiquidation = numberOfLiquidation
        self.totalAmountOfLiquidation = totalAmountOfLiquidation
        self.dailyTransactionAmounts = dailyTransactionAmounts
        self.dailyFrequencyOfTransactions = dailyFrequencyOfTransactions
    
class Token:
    def __init__(self, address, totalSupply, symbol, name, decimal, dailyFrequencyOfTransactions,
                creditScore, price, highestPrice, marketCap, tradingVolume24, lastUpdatedAt):
        self.address = address
        self.totalSupply = totalSupply
        self.symbol = symbol
        self.name = name
        self.decimal =decimal
        self.dailyFrequencyOfTransactions = dailyFrequencyOfTransactions
        self.creditScore = creditScore
        self.price = price
        self.highestPrice = highestPrice
        self.marketCap = marketCap
        self.tradingVolume24 = tradingVolume24
        self.lastUpdatedAt = lastUpdatedAt
        
class LendingPool:
    def __init__(self, address, tokens, supply, borrow):
        self.address = address
        self.tokens = tokens
        self.supply = supply
        self.borrow = borrow

class User:
    def __init__(self):
        pass