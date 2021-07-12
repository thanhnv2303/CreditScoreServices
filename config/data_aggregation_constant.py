class MemoryStorageKeyConstant:
    update_wallet = "update_wallet"
    checkpoint = "checkpoint"
    performance_constant_keys = "performance_constant_keys"


class EventTypeAggregateConstant:
    Borrow = "Borrow"
    Deposit = "Deposit"
    LiquidateBorrow = "LiquidateBorrow"
    LiquidationCall = "LiquidationCall"
    Mint = "Mint"
    Redeem = "Redeem"
    Repay = "Repay"
    RepayBorrow = "RepayBorrow"
    Withdraw = "Withdraw"
    Transfer = "Transfer"


class DepositEventConstant:
    reserve = "reserve"
    onBehalfOf = "onBehalfOf"
    user = "user"
    amount = "amount"


class MintEventConstant:
    minter = "minter"
    mintAmount = "mintAmount"


class BorrowEventVTokenConstant:
    borrower = "borrower"
    borrowAmount = "borrowAmount"


class BorrowEventPoolConstant:
    user = "user"
    onBehalfOf = "onBehalfOf"
    reserve = "reserve"
    amount = "amount"


class RedeemEventConstant:
    redeemer = "redeemer"
    redeemAmount = "redeemAmount"


class WithdrawEventConstant:
    reserve = "reserve"
    user = "user"
    to = "to"
    redeemer = "redeemer"
    amount = "amount"


class RepayEventConstant:
    reserve = "reserve"
    user = "user"
    repayer = "repayer"
    amount = "amount"


class RepayBorrowEventConstant:
    payer = "payer"
    borrower = "borrower"
    repayAmount = "repayAmount"


class LiquidateBorrowEventConstant:
    liquidator = "liquidator"
    borrower = "borrower"
    repayAmount = "repayAmount"
    vTokenCollateral = "vTokenCollateral"
    seizeTokens = "seizeTokens"


class LiquidationCallEventConstant:
    collateralAsset = "collateralAsset"
    debtAsset = "debtAsset"
    user = "user"
    debtToCover = "debtToCover"
    liquidatedCollateralAmount = "liquidatedCollateralAmount"
    liquidator = "liquidator"


class RunOnConstant:
    BSC_MAINNET = "BSC_MAINNET"
    BSC_TESTNET = "BSC_TESTNET"
    ETH_MAINNET = "ETH_MAINNET"
