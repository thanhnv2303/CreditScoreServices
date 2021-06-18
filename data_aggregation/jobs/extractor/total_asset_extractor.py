import logging

from data_aggregation.database.intermediary_database import IntermediaryDatabase
from data_aggregation.jobs.extractor.extractor import Extractor
from utils.to_number import to_int

logger = logging.getLogger("Calculate total asset")


class TotalAssetExtractor(Extractor):
    num_wallet = 0

    def __init__(self, start_block, end_block, checkpoint, web3, database=IntermediaryDatabase()):
        super(TotalAssetExtractor, self).__init__(start_block, end_block, checkpoint, web3, database)
        self.statistic_credit["total_asset_list"] = {}
        self.statistic_credit_lock = False

    def extract(self, wallet_data):

        # start_time = time.time()
        address = wallet_data.get("address")
        if not wallet_data.get("balances") and not wallet_data.get("lending_info"):
            # self.database_common.delete_wallet(address)
            return
        ### get wallet at credit database_common
        wallet_credit = self.database.get_wallet_credit(address)
        if not wallet_credit:
            wallet_credit = wallet_data

        ### get info credit from start_block to end_block

        ### pruning info out of range start block and end block
        if not wallet_credit.get("lending_infos"):
            return
        for address in wallet_credit.get("lending_infos"):
            i = 0
            lending_info_address = wallet_data.get("lending_infos")[address]
            while i < len(lending_info_address):
                block_num = lending_info_address[i].get("block_number")
                if block_num >= self.start_block:
                    break
                i = i + 1

            wallet_credit.get("lending_infos")[address] = lending_info_address[i:]

        total_asset = 0
        for address in wallet_credit.get("lending_infos"):
            wallet_lending_token = wallet_credit.get("lending_infos").get(address)

            start_block = self.start_block
            change_times = len(wallet_lending_token)
            avg_value = 0
            for i in range(change_times):
                balance = to_int(wallet_lending_token[i].get("balance"))
                supply = to_int(wallet_lending_token[i].get("supply"))
                borrow = to_int(wallet_lending_token[i].get("borrow"))
                amount = balance + supply - borrow

                if i == change_times - 1:
                    end_block = self.end_block
                else:
                    end_block = wallet_lending_token[i + 1].get("block_number")
                if end_block < self.start_block:
                    continue
                accumulate_value = self.credit_score_service.token_amount_to_usd(address, amount) * (
                        end_block - start_block)
                if accumulate_value > 0:
                    avg_value += accumulate_value
                start_block = end_block
            avg_value = avg_value / (self.end_block - self.start_block)

            total_asset += avg_value

        wallet_credit["total_asset"] = total_asset
        if total_asset <= 0:
            return
        ### update other info
        wallet_credit["at_block_number"] = wallet_data.get("at_block_number")
        wallet_credit["balances"] = wallet_data.get("balances")
        wallet_credit["transactions"] = wallet_data.get("transactions")
        wallet_credit["accumulate"] = wallet_data.get("accumulate")
        wallet_credit["accumulate_history"] = wallet_data.get("accumulate_history")
        wallet_credit["lending_info"] = wallet_data.get("lending_info")

        self.database.update_wallet_credit(wallet_credit)

        list_name = "total_asset_list"
        wallet_address = wallet_data.get("address")

        self.add_to_statistic_list(list_name, wallet_address, total_asset)

