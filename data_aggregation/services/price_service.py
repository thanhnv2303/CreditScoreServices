import json
import logging
import os

from config.constant import CreditScoreConstant, TokenConstant
from data_aggregation.database.intermediary_database import IntermediaryDatabase
from data_aggregation.services.update_date_token_credit_score import update_token_credit_score
from utils.to_number import to_int, to_float

logger = logging.getLogger('Credit Score Service V0.3.0')


class PriceService:

    def __init__(self, database=IntermediaryDatabase(), list_token_filter="artifacts/token_credit_info/listToken.txt",
                 token_info="artifacts/token_credit_info/infoToken.json"):
        self.database = database
        self.fix_prices = {
            "0x": {
                "price": 343.30,  ### fix price for BNB
                "decimals": 18
            }
        }

        self.file_input = list_token_filter
        self.file_output = token_info
        cur_path = os.path.dirname(os.path.realpath(__file__)) + "/../../"
        path_market = cur_path + token_info

        with open(path_market, "r") as file:
            try:
                self.tokens_market = json.load(file)
            except:
                self.update_token_market_info(fileInput=list_token_filter, fileOutput=token_info)
                with open(path_market, "r") as file:
                    self.tokens_market = json.load(file)

        self.tokens_market["0x"] = {
            "symbol": "BNB",
            "price": "380.17",
            "credit_score": 1000.0,
            "market_cap": 7555675507.00,
            "decimals": 18
        }

        self.balance_threshold = CreditScoreConstant.balance_threshold
        self.supply_threshold = CreditScoreConstant.supply_threshold
        self.threshold = CreditScoreConstant.threshold
        self.total_borrow_threshold = CreditScoreConstant.total_borrow_threshold

    def token_amount_to_usd(self, token_address, amount):
        token = self.tokens_market.get(token_address)
        if not token:
            return 0
        decimals = to_int(token.get(TokenConstant.decimals))
        if not decimals:
            decimals = 18
        price = to_float(token.get(TokenConstant.price))

        return to_int(amount) * price / 10 ** decimals

    def get_total_value(self, token_dict_value):
        """
        Tính tổng giá trị theo usd của một map token
        vd:
        token_token_dict_value ={
            "0x": "160032960619666255931",
            "0xe9e7cea3dedca5984780bafc599bd69add087d56": "31155448912850494165922",
            "0x7130d2a12b9bcbfae4f2634d864a1ee1ce3ead9c": "714147538522475001",
            "0x55d398326f99059ff775485246999027b3197955": "17969310173334216785083",
            "0x1d2f0da169ceb9fc7b3144628db156f3f6c60dbe": "0",
            "0x7083609fce4d1d8dc0c979aab8c869ea2c873402": "168771978073903870",
            "0xbf5140a22578168fd562dccf235e5d43a02ce9b1": "2008751971899470006",
            "0x2170ed0880ac9a755fd29b2688956bd959f933f8": "17930821393328899321",
            "0x3ee2200efb3400fabb9aacf31297cbdd1d435d47": "2394530795346156909"
        }

        :param token_dict_value:
        :return:
        """
        total_value = 0
        for token_address in token_dict_value:
            value = token_dict_value[token_address]
            total_value += self.token_amount_to_usd(token_address, value)
        return total_value

    def update_token_market_info(self, fileInput='artifacts/token_credit_info/listToken.txt',
                                 fileOutput='artifacts/token_credit_info/infoToken.json'):
        update_token_credit_score(fileInput=fileInput, fileOutput=fileOutput, database=self.database)
        return 0

    def update_token_info(self):
        file_input = self.file_input
        file_output = self.file_output
        update_token_credit_score(fileInput=file_input, fileOutput=file_output, database=self.database)
        return 0
