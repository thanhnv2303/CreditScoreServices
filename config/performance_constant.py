import inspect


class PerformanceConstant:
    ### read mongo
    block_number_to_time_stamp = "1.block_number_to_time_stamp"
    get_latest_block_update = "1.get_latest_block_update"
    get_oldest_block_update = "1.get_oldest_block_update"
    get_first_create_wallet = "1.get_first_create_wallet"
    get_transfer_native_token_tx_in_block = "1.get_transfer_native_token_tx_in_block"
    get_events_at_of_smart_contract = "1.get_events_at_of_smart_contract"
    get_wallet = "1.get_wallet"
    get_token = "1.get_token"
    get_num_event_of_smart_contract_after_block = "1.get_num_event_of_smart_contract_after_block"

    ### read neo4j
    get_token_prices = "2.get_token_prices"
    get_wallet_created_at = "2.get_wallet_created_at"
    get_balance_100 = "2.get_balance_100"
    get_daily_transaction_amount_100 = "2.get_daily_transaction_amount_100"
    get_daily_daily_frequency_of_transaction = "2.get_daily_daily_frequency_of_transaction"
    get_num_of_liquidation_100 = "2.get_num_of_liquidation_100"
    get_total_amount_liquidation_100 = "2.get_total_amount_liquidation_100"
    get_deposit_100 = "2.get_deposit_100"
    get_borrow_100 = "2.get_borrow_100"
    get_info_relationship = "2.get_info_relationship"

    ### write neo4j
    update_wallet_token = "3.update_wallet_token"
    update_wallet_token_deposit_and_borrow = "3.update_wallet_token_deposit_and_borrow"
    update_wallet_created_at = "3.update_wallet_created_at"
    update_balance_100 = "3.update_balance_100"
    update_daily_transaction_amount_100 = "3.update_daily_transaction_amount_100"
    update_daily_frequency_of_transaction = "3.update_daily_frequency_of_transaction"
    update_num_of_liquidation_100 = "3.update_num_of_liquidation_100"
    update_total_amount_liquidation_100 = "3.update_total_amount_liquidation_100"
    update_deposit_100 = "3.update_deposit_100"
    update_borrow_100 = "3.update_borrow_100"
    update_daily_frequency_of_transactions = "3.update_daily_frequency_of_transactions"
    create_transfer_relationship = "3.create_transfer_relationship"
    create_deposit_relationship = "3.create_deposit_relationship"
    create_borrow_relationship = "3.create_borrow_relationship"
    create_repay_relationship = "3.create_repay_relationship"
    create_withdraw_relationship = "3.create_withdraw_relationship"
    create_liquidate_relationship = "3.create_liquidate_relationship"

    ### read write db
    read_mongo_time = "4.read_mongo_time"
    read_neo4j_time = "4.read_neo4j_time"
    write_neo4j_time = "4.write_neo4j_time"
    ### job
    UpdateTokenJob = "5.UpdateTokenJob"
    AggregateNativeTokenTransferJob = "5.AggregateNativeTokenTransferJob"
    AggregateSmartContractJob = "5.AggregateSmartContractJob"
    AggregateWalletJob = "5.AggregateWalletJob"

    def get_all_attr(self):
        obj = PerformanceConstant
        attr = [getattr(PerformanceConstant, a) for a in dir(obj) if
                not a.startswith('__') and not callable(getattr(obj, a))]
        attr.sort()
        return attr
