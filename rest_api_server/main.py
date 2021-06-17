import os
import sys

TOP_DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.insert(0, os.path.join(TOP_DIR, './'))

from services.log_services import config_log
import sys

from aiohttp import web
import logging

from config.config import RestApiServer
from database.database import Database
from route_handler import RouteHandler

LOGGER = logging.getLogger("Credit Score Rest Api")

config_log(logging.INFO)


def start_rest_api(host, port, database):
    app = web.Application()
    # WARNING: UNSAFE KEY STORAGE
    # In a production application these keys should be passed in more securely
    app['aes_key'] = 'ffffffffffffffffffffffffffffffff'
    app['secret_key'] = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890'
    handler = RouteHandler(database)

    handler.add_route(app)
    LOGGER.info(f"Start Listen at http://{host}:{port}")
    web.run_app(
        app,
        host=host,
        port=port,
        access_log=LOGGER,
        access_log_format='%r: %s status, %b size, in %Tf s')


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    try:

        database = Database()

        host_server = RestApiServer.HOST
        port_server = RestApiServer.PORT

        start_rest_api(host_server, port_server, database)
    except Exception as err:  # pylint: disable=broad-except
        LOGGER.exception(err)
        sys.exit(1)
    finally:
        database.disconnect()
