class BlockConstant:
    gas_limit = "gas_limit"
    gas_used = "gas_used"
    number = "number"
    block_timestamp = "block_timestamp"
    timestamp = "timestamp"


class TransactionConstant:
    gas = "gas"
    gas_price = "gas_price"
    value = "value"
    input = "input"
    hash = "hash"
    transaction_hash = "transaction_hash"
    related_wallets = "related_wallets"
    block_number = "block_number"
    from_address = "from_address"
    to_address = "to_address"
    block_timestamp = "block_timestamp"


class TokenConstant:
    value = "value"
    contract_address = "contract_address"
    type = "type"
    native_token = "0x"
    event_type = "event_type"
    total_supply = "total_supply"
    address = "address"
    symbol = "symbol"
    decimals = 'decimals'
    name = "name"
    block_number = "block_number"
    price = "price"


class TokenTypeConstant:
    Transfer = "Transfer"


class WalletConstant:
    address = "address"
    at_block_number = "at_block_number"
    address_nowhere = '0x0000000000000000000000000000000000000000'
    new_balance_of_concerning_token = "new_balance_of_concerning_token"
    old_balance_of_concerning_token = "old_balance_of_concerning_token"
    balance = "balance"
    supply = "supply"
    borrow = "borrow"
    unit_token = "unit_token"
    credit_score = "credit_score"


class ExportItemConstant:
    type = "type"


class ExportItemTypeConstant:
    transaction = "transaction"
    block = "block"
    token_transfer = "token_transfer"
    event = "event"
    token = "token"


class LoggerConstant:
    KnowledgeGraphExporter = "KnowledgeGraphExporter"
    ExportBlocksJob = "ExportBlocksJob"
    AggregateNativeTokenTransferJob = "AggregateNativeTokenTransferJob"
    AggregateEventJob = "AggregateEventJob"
    AggregateSmartContractJob = "AggregateSmartContractJob"
    AggregateWalletJob = "AggregateWalletJob"
    EthService = "EthService"
    EthLendingService = "EthLendingService"
    UpdateTokenJob = "UpdateTokenJob"


class EthKnowledgeGraphStreamerAdapterConstant:
    tokens_filter_file_default = "artifacts/token_filter"
    event_abi_dir_default = "artifacts/event-abi"
    batch_size_default = 100
    max_workers_default = 8
    partition_batch_size_default = 1000


class EventConstant:
    name = "name"
    saveName = "saveName"
    isLending = "isLending"
    inputs = "inputs"
    type = "type"
    event = "event"
    log_index = "log_index"
    event_type = "event_type"
    contract_address = "contract_address"


class EventInputConstant:
    name = "name"
    type = "type"
    address = "address"
    indexed = "indexed"


class EventFilterConstant:
    fromBlock = "fromBlock"
    toBlock = "toBlock"
    topics = "topics"


class TimeUpdateConstant:
    token_update_hour = 3
    token_update_minute = 5


class MongoIndexConstant:
    tx_id = "tx_id"
    transfer_tx_id = "transfer_tx_id"
    transfer_block_number = "transfer_block_number"
    wallet_address = "wallet_address"
    tx_to_address = "tx_to_address"
    tx_from_address = "tx_from_address"
    tx_block_timestamp = "block_timestamp_1"
    blocks_number = "block_number"
    native_transfer_block_number = "native_transfer_block_number"


class LendingTypeConstant:
    lendingType = "lendingType"
    VTOKEN = "VTOKEN"
    TRAVA = "TRAVA"
    ERC20 = "ERC20"
    LENDING_POOL = "LENDING_POOL"


class LendingPoolConstant:
    DECIMALS = 8
    total_supply = "total_supply"
    total_borrow = "total_borrow"
    address = "address"
    name = "name"
    block_number = "block_number"


class CreditScoreConstant:
    balance_threshold = 1000
    supply_threshold = 1000
    threshold = 1000
    total_borrow_threshold = 1000


class RelationshipConstant:
    from_address = "from_address"
    to_address = "to_address"
    tx_id = "tx_id"
    amount = "amount"
    token = "token"
    label = "label"


class Neo4jWalletConstant:
    address = "address"
    lastUpdatedAt = "lastUpdatedAt"
    creditScore = "creditScore"
    tokens = "tokens"
    tokenBalances = "tokenBalances"
    balanceInUSD = "balanceInUSD"
    balanceChangeLogTimestamps = "balanceChangeLogTimestamps"
    balanceChangeLogValues = "balanceChangeLogValues"
    createdAt = "createdAt"
    depositTokens = "depositTokens"
    depositTokenBalances = "depositTokenBalances"
    depositInUSD = "depositInUSD"
    depositChangeLogTimestamps = "depositChangeLogTimestamps"
    depositChangeLogValues = "depositChangeLogValues"
    borrowTokens = "borrowTokens"
    borrowTokenBalances = "borrowTokenBalances"
    borrowInUSD = "borrowInUSD"
    borrowChangeLogTimestamps = "borrowChangeLogTimestamps"
    borrowChangeLogValues = "borrowChangeLogValues"
    numberOfLiquidation = "numberOfLiquidation"
    totalAmountOfLiquidation = "totalAmountOfLiquidation"
    dailyTransactionAmounts = "dailyTransactionAmounts"
    dailyFrequencyOfTransactions = "dailyFrequencyOfTransactions"


class Neo4jTokenConstant:
    address = "address"
    totalSupply = "totalSupply"
    symbol = "symbol"
    name = "name"
    decimal = "decimal"
    dailyFrequencyOfTransactions = "dailyFrequencyOfTransactions"
    creditScore = "creditScore"
    price = "price"
    highestPrice = "highestPrice"
    marketCap = "marketCap"
    tradingVolume24 = "tradingVolume24"
    lastUpdatedAt = "lastUpdatedAt"


class Neo4jLendingPoolConstant:
    address = "address"
    tokens = "tokens"
    supply = "supply"
    borrow = "borrow"


class Neo4jTransferConstant:
    transactionID = "transactionID"
    timestamp = "timestamp"
    fromWallet = "fromWallet"
    toWallet = "toWallet"
    token = "token"
    value = "value"


class Neo4jDepositConstant:
    transactionID = "transactionID"
    timestamp = "timestamp"
    fromWallet = "fromWallet"
    toAddress = "toAddress"
    token = "token"
    value = "value"


class Neo4jBorrowConstant:
    transactionID = "transactionID"
    timestamp = "timestamp"
    fromWallet = "fromWallet"
    toAddress = "toAddress"
    token = "token"
    value = "value"


class Neo4jRepayConstant:
    transactionID = "transactionID"
    timestamp = "timestamp"
    fromWallet = "fromWallet"
    toAddress = "toAddress"
    token = "token"
    value = "value"


class Neo4jWithdrawConstant:
    transactionID = "transactionID"
    timestamp = "timestamp"
    fromWallet = "fromWallet"
    toAddress = "toAddress"
    token = "token"
    value = "value"


class Neo4jLiquidateConstant:
    transactionID = "transactionID"
    timestamp = "timestamp"
    protocol = "protocol"
    fromWallet = "fromWallet"
    toWallet = "toWallet"
    fromBalance = "fromBalance"
    fromAmount = "fromAmount"
    toBalance = "toBalance"
    toAmount = "toAmount"
