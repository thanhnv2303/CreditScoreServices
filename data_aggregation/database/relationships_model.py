class Transfer:
    def __init__(self, transactionID, timestamp, fromWallet, toWallet, token, value):
        self.transactionID = transactionID
        self.timestamp = timestamp
        self.fromWallet = fromWallet
        self.toWallet = toWallet
        self.token = token
        self.value = value

class Deposit:
    def __init__(self, transactionID, timestamp, fromWallet, toAddress, token, value):
        self.transactionID = transactionID
        self.timestamp = timestamp
        self.fromWallet = fromWallet
        self.toAddress = toAddress
        self.token = token
        self.value = value

class Borrow:
    def __init__(self, transactionID, timestamp, fromWallet, toAddress, token, value):
        self.transactionID = transactionID
        self.timestamp = timestamp
        self.fromWallet = fromWallet
        self.toAddress = toAddress
        self.token = token
        self.value = value

class Repay:
    def __init__(self, transactionID, timestamp, fromWallet, toAddress, token, value):
        self.transactionID = transactionID
        self.timestamp = timestamp
        self.fromWallet = fromWallet
        self.toAddress = toAddress
        self.token = token
        self.value = value

class Withdraw:
    def __init__(self, transactionID, timestamp, fromWallet, toAddress, token, value):
        self.transactionID = transactionID
        self.timestamp = timestamp
        self.fromWallet = fromWallet
        self.toAddress = toAddress
        self.token = token
        self.value = value

class Liquidate:
    def __init__(self, transactionID, timestamp, protocol, fromWallet, toWallet, fromBalance, fromAmount, toBalance, toAmount):
        self.transactionID = transactionID
        self.timestamp = timestamp
        self.protocol = protocol
        self.fromWallet = fromWallet
        self.toWallet = toWallet
        self.fromBalance = fromBalance
        self.fromAmount = fromAmount
        self.toBalance = toBalance
        self.toAmount = toAmount

class Hold:
    def __init__(self):
        pass
