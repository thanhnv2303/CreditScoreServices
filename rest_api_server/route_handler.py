import datetime
import logging
import time
from json.decoder import JSONDecodeError

import aiohttp_cors as aiohttp_cors
from aiohttp.web import json_response

from calculate_credit_score.test.calculate_credit_score_one import CalculateCreditScoreOneWallet
from calculate_credit_score.test.estimate_credit_score_one import EstimateCreditScore
from config.config import RestApiServer
from config.rest_api_constant import RouteApiConstant
from errors import ApiBadRequest, ApiInternalError
from rest_api_server.database.database import Database

LOGGER = logging.getLogger(__name__)


class RouteHandler(object):
    def __init__(self, database=Database()):
        self._database = database

    async def get_credit_score(self, request):
        address = request.match_info.get('address', '')
        address = str(address).lower()
        try:
            calc = CalculateCreditScoreOneWallet(address)
            credit_score = calc.updateCreditScore()
            return json_response(
                {
                    "address": address,
                    'credit_score': credit_score,
                })
        except Exception as e:
            LOGGER.info(e)
            return ApiInternalError(str(e))

    async def estimate_credit_score(self, request):
        address = request.match_info.get('address', '')
        address = str(address).lower()

        try:
            type_transaction = int(request.rel_url.query['type_transaction'])
            amount = int(request.rel_url.query['amount'])
            time_estimates = int(request.rel_url.query['time_estimates'])
            calc = EstimateCreditScore(address, type_transaction, amount, time_estimates)
            credit_score = calc.newCreditScore()
            return json_response(
                {
                    "address": address,
                    'estimate_credit_score': credit_score,
                })
        except Exception as e:
            LOGGER.info(e)
            return ApiInternalError(str(e))

    async def update_credit_score(self, request):
        address = request.match_info.get('address', '')
        credit_score = self._database.update_credit_score(address)
        return json_response(
            {
                "address": address,
                'credit_score': credit_score,
            })

    async def visuzlize_data_sample(self, request):
        return json_response(
            {
                'msg': "Visualize Data Sample is building api, wait for a little time",
            })

    async def get_token_credit_score(self, request):
        address = request.match_info.get('address', '')
        credit_score = self._database.get_token_score(address)
        return json_response(
            {
                "token_address": address,
                'credit_score': credit_score,
            })

    def add_route(self, app):
        app.router.add_get(
            RestApiServer.API_VERSION + RouteApiConstant.get_credit_score_path + RouteApiConstant.address,
            self.get_credit_score)

        app.router.add_get(
            RestApiServer.API_VERSION + RouteApiConstant.estimate_credit_score_path + RouteApiConstant.address,
            self.estimate_credit_score)

        app.router.add_get(
            RestApiServer.API_VERSION + RouteApiConstant.update_credit_score_path + RouteApiConstant.address,
            self.update_credit_score)

        app.router.add_get(RestApiServer.API_VERSION + RouteApiConstant.visualize_data_sample_path,
                           self.visuzlize_data_sample)

        app.router.add_get(
            RestApiServer.API_VERSION + RouteApiConstant.get_token_credit_score_path + RouteApiConstant.address,
            self.get_token_credit_score)

        cors = aiohttp_cors.setup(app, defaults={
            "*": aiohttp_cors.ResourceOptions(
                allow_credentials=True,
                expose_headers="*",
                allow_headers="*",
            )
        })

        # Configure CORS on all routes.
        for route in list(app.router.routes()):
            cors.add(route)


async def decode_request(request):
    try:
        return await request.json()
    except JSONDecodeError:
        raise ApiBadRequest('Improper JSON format')


def validate_fields(required_fields, body):
    for field in required_fields:
        if body.get(field) is None:
            raise ApiBadRequest(
                "'{}' parameter is required".format(field))


def get_time():
    dts = datetime.datetime.utcnow()
    return round(time.mktime(dts.timetuple()) + dts.microsecond / 1e6)

#
# def generate_auth_token(secret_key, public_key):
#     serializer = Serializer(secret_key)
#     token = serializer.dumps({'public_key': public_key})
#     return token.decode('ascii')
#
#
# def deserialize_auth_token(secret_key, token):
#     serializer = Serializer(secret_key)
#     return serializer.loads(token)
