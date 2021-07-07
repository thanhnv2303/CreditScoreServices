class Transfer:
    def __init__(self, transactionID, timestamp, fromWallet, toWallet, token, value, valueUsd):
        self.transactionID = transactionID
        self.timestamp = timestamp
        self.fromWallet = fromWallet
        self.toWallet = toWallet
        self.token = token
        self.value = value
        self.valueUsd = valueUsd


class Deposit:
    def __init__(self, transactionID, timestamp, fromWallet, toAddress, token, value, valueUsd, relatedWallets):
        self.transactionID = transactionID
        self.timestamp = timestamp
        self.fromWallet = fromWallet
        self.toAddress = toAddress
        self.token = token
        self.value = value
        self.valueUsd = valueUsd
        self.relatedWallets = relatedWallets


class Borrow:
    def __init__(self, transactionID, timestamp, fromWallet, toAddress, token, value, valueUsd, relatedWallets):
        self.transactionID = transactionID
        self.timestamp = timestamp
        self.fromWallet = fromWallet
        self.toAddress = toAddress
        self.token = token
        self.value = value
        self.valueUsd = valueUsd
        self.relatedWallets = relatedWallets


class Repay:
    def __init__(self, transactionID, timestamp, fromWallet, toAddress, token, value, valueUsd, relatedWallets):
        self.transactionID = transactionID
        self.timestamp = timestamp
        self.fromWallet = fromWallet
        self.toAddress = toAddress
        self.token = token
        self.value = value
        self.valueUsd = valueUsd
        self.relatedWallets = relatedWallets


class Withdraw:
    def __init__(self, transactionID, timestamp, fromWallet, toAddress, token, value, valueUsd, relatedWallets):
        self.transactionID = transactionID
        self.timestamp = timestamp
        self.fromWallet = fromWallet
        self.toAddress = toAddress
        self.token = token
        self.value = value
        self.valueUsd = valueUsd
        self.relatedWallets = relatedWallets


class Liquidate:
    def __init__(self, transactionID, timestamp, protocol, fromWallet, toWallet, fromBalance, fromAmount, toBalance,
                 toAmount):
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
