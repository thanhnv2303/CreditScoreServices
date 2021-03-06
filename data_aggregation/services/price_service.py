import logging

from config.constant import CreditScoreConstant, TokenConstant
from data_aggregation.database.intermediary_database import IntermediaryDatabase
from data_aggregation.database.klg_database import KlgDatabase
from data_aggregation.services.update_date_token_credit_score import update_token_credit_score
from utils.to_number import to_int, to_float

logger = logging.getLogger('Credit Score Service V0.3.0')


class PriceService:

    def __init__(self, database=IntermediaryDatabase(), klg_database=KlgDatabase()):
        self.database = database
        self.klg_database = klg_database
        self.fix_prices = {
            "0x": {
                "price": 343.30,  ### fix price for BNB
                "decimals": 18
            }
        }
        self.tokens_market = {}

        ### fix price
        self.tokens_market["0x"] = {
            "symbol": "BNB",
            "price": "380.17",
            "credit_score": 1000.0,
            "market_cap": 7555675507.00,
            "decimals": 18
        }
        self.tokens_market["0xcedc3b4d3c4359a1f842cf94d3418fedb105669f"] = {
            "symbol": "DAI",
            "price": "1.1",
            "credit_score": 1000.0,
            "market_cap": 7555675507.00,
            "decimals": 18
        }
        self.tokens_market["0x059ba0204de65bddb172d55d6a53074ea98d7917"] = {
            "symbol": "USDT",
            "price": "12.1",
            "credit_score": 1000.0,
            "market_cap": 7555675507.00,
            "decimals": 18
        }
        self.tokens_market["0xfc3fb4aa34c1720d5a50eb70b60d3118cc47636c"] = {
            "symbol": "USDC",
            "price": "12.1",
            "credit_score": 1000.0,
            "market_cap": 7555675507.00,
            "decimals": 18
        }
        self.tokens_market["0xfc3fb4aa34c1720d5a50eb70b60d3118cc47636c"] = {
            "symbol": "USDC",
            "price": "12.1",
            "credit_score": 1000.0,
            "market_cap": 7555675507.00,
            "decimals": 18
        }
        # self.update_token_market_info()
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
        T??nh t???ng gi?? tr??? theo usd c???a m???t map token
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

    def update_token_market_info(self):
        token_prices = self.klg_database.get_token_prices()
        for token_address in token_prices:
            token = self.database.get_token(token_address)
            if token:
                self.tokens_market[token_address] = token
            else:
                self.tokens_market[token_address] = {}
            self.tokens_market[token_address][TokenConstant.price] = token_prices[token_address]
        # print(self.tokens_market)
        return 0
# price_service = PriceService()
