import ccxt
import time
import psycopg2
import logging
import json
from datetime import datetime


class DataReceiver:

    """
    Receiving data through exchange API.
    Inserting into PostgreSQL AWS DB.

    Defaults:
    instrument = [ETH-USDT-SWAP]
    connection_settings = dict - defaults from postgres config file.
    limit = 100 (amount of 1 minute chart bars)
    time_frame = 1 (string - 1 minute)
    db_name = database name
    logs_file = path to logs file

    DataReceiver.start() to start data gathering.
    """

    def __init__(self, logs_file='./logs/logs.csv',
                       db_name='okex',
                       time_frame='1m',
                       limit=100,
                       instrument='ETH-USDT-SWAP',
                       connection_settings=None,
                       db_credentioals_file='./db_config/config.json',
                       create_table=False):

        if connection_settings is None:
            with open(db_credentioals_file, 'r') as f:
                credentials = json.load(f)
                connection_settings = {
                    'database': credentials["database"],
                    'user': credentials["user"],
                    'password': credentials["password"],
                    'host': credentials["host"],
                    'port': credentials["port"]}

        self.connection_settings = connection_settings
        self.instrument = instrument
        self.exchange = ccxt.okex()
        self.connection = psycopg2.connect(**self.connection_settings)
        self.connection.autocommit = True
        self.cursor = self.connection.cursor()
        self.limit = limit
        self.time_frame = time_frame
        self.db_name = db_name
        self.logs_file = logs_file
        self.create_table = create_table

        logging.basicConfig(filename=self.logs_file, level=logging.INFO)


    def start(self):
        """

        1. Receiving data points/bars (with a limit) from OKEX exchange for a specified ticker.
        2. Go through each value, insert it to AWS PostgreSQL DB. If time value is not unique - pass.

        :return: None
        """

        minute = 60
        add_to_limit = 10
        limit = self.limit

        if self.create_table:
            self.cursor.execute(
                """
                create table okex(
                    time varchar(15),
                    open varchar(10),
                    high varchar(10),
                    low varchar(10),
                    close varchar(10),
                    volume varchar(15)
                    )
                """
            )

        while True:
            try:
                data = self.exchange.fetch_ohlcv(symbol=self.instrument, timeframe='1m', limit=limit)
                limit = self.limit
            except Exception as e:
                time.sleep(minute)
                limit += add_to_limit  # in case of an api bug, i want to widen the history range from 100 to 110
                                       # to take all data which wasn't taken due to the bug.
                logging.info(e)
                logging.info(datetime.now())
            else:
                for value in data:
                    query = 'insert into {} values {}'.format(self.db_name, str(tuple(value)))
                    try:
                        self.cursor.execute(query)
                    except psycopg2.errors.UniqueViolation:
                        pass
                    except Exception as e:
                        logging.info(e)
                        logging.info(datetime.now())

            time.sleep(minute * limit)


if __name__ == '__main__':
	data_receiver = DataReceiver()
	data_receiver.start()
