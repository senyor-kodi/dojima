import pandas as pd
import numpy as np
import json
import time
import yaml

import ccxt
import ccxt.async_support as ccxt_async
import ccxt.pro

import asyncio
from asyncio import Event
import aiomysql
import aiohttp
import aio_pika

from config import CEX

from decimal import Decimal, getcontext

import warnings

# Filter out specific warning messages
warnings.filterwarnings(
    'ignore', category=Warning, message='Data truncated for column'
)
warnings.filterwarnings('ignore', category=Warning, message='Duplicate entry')

from logger_config import logger

# Set the precision and scale
getcontext().prec = 20
getcontext().Emax = 8


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(
                *args, **kwargs
            )
        return cls._instances[cls]


class HAL(metaclass=Singleton):
    def init(
        self,
        dbhost: str = 'localhost',
        dbport: str = None,
        dbname: str = 'dojima_db1',
        dbuser: str = 'root',
        dbpass: str = 'provenzal10',
        exchanges: list = ['binance', 'okx'],
        bar_data: bool = True,
        orderbook_data: bool = False,
        tick_data: bool = False,
        funding_data: bool = True,
        timeframes: set = {'5m', '1h'},
        backfill_window: int = None,
        logger=None,
        rmq_url: str = 'amqp://guest:guest@127.0.0.1/',
        name: str = 'hal',
        **kwargs,
    ):
        self.name = name

        self._lock = asyncio.Lock()

        self.ccxt = {}
        self.ccxt_pro = {}

        self.exchanges = exchanges

        for exchange_name in self.exchanges:
            self.new_exchange(exchange_name)

        self.db_config = {
            'host': dbhost,
            'user': dbuser,
            'password': dbpass,
            'db': dbname,
        }

        self.timeframes = timeframes
        self.bar = bar_data
        self.orderbook = orderbook_data
        self.tick = tick_data
        self.funding = funding_data

        self.backfill_window = backfill_window

        self.logger = logger

        self.event_exchanges = set()
        self.event_symbols = set()
        self.strat_timeframes = set(['1h'])

        self.is_running = False

        self.rmq_url = rmq_url
        self.rmq_topic = self.name

        self.strats_data = {}
        self.symbol_tasks = {}

        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 5

    def new_exchange(self, exchange_name):

        # Read the YAML file
        with open('', 'r') as file: # replace with yaml file (or equivalent) with credentials data
            exchange_credentials = yaml.safe_load(file)

        # Access the credentials by exchange name
        api_key = exchange_credentials[exchange_name]['API-KEY']
        api_secret = exchange_credentials[exchange_name]['API-SECRET']

        spot_symbols = CEX[exchange_name]['SPOT-SYMBOLS']
        perps_symbols = CEX[exchange_name]['PERPS-SYMBOLS']
        symbols = spot_symbols + perps_symbols

        exchange_params = {
            'apiKey': api_key,
            'secret': api_secret,
            'enableRateLimit': True,
            'options': {'keepAlive': True},
        }

        if exchange_name == 'okx':
            exchange_params['password'] = exchange_credentials[exchange_name][
                'PASSPHRASE'
            ]

        if exchange_name == 'woo':
            exchange_params['uid'] = exchange_credentials[exchange_name]['UID']

        self.ccxt[exchange_name] = {
            'exchange': getattr(ccxt_async, exchange_name)(exchange_params),
            'exchange_name': exchange_name,
            'spot_symbols': set(spot_symbols),
            'perps_symbols': set(perps_symbols),
            'symbols': set(symbols),
        }

        self.ccxt_pro[exchange_name] = {
            'exchange': getattr(ccxt.pro, exchange_name)(exchange_params),
            'exchange_name': exchange_name,
            'spot_symbols': set(spot_symbols),
            'perps_symbols': set(perps_symbols),
            'symbols': set(symbols),
        }

    # -------------------------------------------
    #   RabbitMQ
    # -------------------------------------------

    async def init_rmq(self):
        self.rmq_conn = await aio_pika.connect_robust(self.rmq_url)
        self.rmq_channel = await self.rmq_conn.channel()
        self.rmq_exchange = await self.rmq_channel.declare_exchange(
            self.name, aio_pika.ExchangeType.DIRECT
        )
        self.rmq_queue = await self.rmq_channel.declare_queue(self.name)
        await self.rmq_queue.bind(self.rmq_exchange, routing_key=self.name)
        self.logger.info('RabbitMQ Hal exchange and queue initialised')

    async def purge_queue(self):
        await self.rmq_queue.purge()

    async def publish_message(self, message, routing_key):
        message_str = json.dumps(message)
        await self.rmq_exchange.publish(
            aio_pika.Message(body=message_str.encode()), routing_key
        )

    async def consume_messages(self):
        try:
            while True:
                async with self.rmq_queue.iterator() as queue_iter:
                    async for message in queue_iter:
                        asyncio.create_task(self.process_message(message))
        except KeyboardInterrupt:
            print('Stopping Hal...')
            self.is_running = False

    async def process_message(self, message):
        async with self._lock:
            async with message.process():
                data = json.loads(message.body.decode())
                routing_key = message.routing_key
                strat_routing_key = data['strat_name']
                self.logger.info(
                    f"Received message of type {data['action']} from strat {data['strat_name']}"
                )
                try:
                    if 'action' in data:
                        if data['action'] == 'add_symbols':
                            await self.add_symbols_to_db(
                                strat_name=strat_routing_key,
                                exchange=data['exchange'],
                                symbols=data['symbols'],
                                timeframe=data['timeframe'],
                                strat_type=data['strat_type'],
                                routing_key=strat_routing_key,
                            )
                            await self.stream_new_symbols(
                                strat_name=strat_routing_key,
                                exchange=data['exchange'],
                                symbols=data['symbols'],
                                timeframe=data['timeframe'],
                                strat_type=data['strat_type'],
                                routing_key=strat_routing_key,
                            )
                        elif data['action'] == 'remove_symbols':
                            await self.remove_symbols(data['strat_name'])
                        elif data['action'] == 'backfill':

                            symbols = data['symbols']
                            if not isinstance(symbols, list):
                                symbols = [symbols]

                            await self.symbol_backfill(
                                exchange_name=data['exchange'],
                                symbols=symbols,
                                timeframe=data['timeframe'],
                                window=data['window'],
                                strat_name=data['strat_name'],
                            )
                        elif data['action'] == 'hal_running':
                            await self.pub_hal_running(
                                routing_key=strat_routing_key
                            )
                        else:
                            self.logger.error(
                                f"No message type {data['action']} available for hal"
                            )

                except Exception as e:
                    self.logger.error(
                        f'Exception raised while processing message: {e}'
                    )

    async def pub_hal_running(self, routing_key):
        message = {
            'type': 'hal_running',
            'is_running': await self._is_hal_running(),
        }
        await self.publish_message(message=message, routing_key=routing_key)

    async def _is_hal_running(self):
        return self.is_running

    async def add_symbols_to_db(
        self,
        strat_name,
        exchange,
        symbols,
        timeframe,
        strat_type,
        routing_key,
    ):
        self.strats_data[strat_name] = {
            'symbols': symbols,
            'exchange': exchange,
            'timeframe': timeframe,
            'type': strat_type,
        }
        self.logger.info('Starting insert_exchange')
        await self.insert_exchange(exchange_name=exchange)
        self.logger.info('Finished insert_exchange')

        self.logger.info('Starting insert_new_symbols')
        await self.insert_new_symbols(
            exchange_name=exchange, symbols=symbols, symbol_type=strat_type
        )
        self.logger.info('Finished insert_new_symbols')

    async def stream_new_symbols(
        self,
        strat_name,
        exchange,
        symbols,
        timeframe,
        strat_type,
        routing_key,
    ):
        pool = await aiomysql.create_pool(**self.db_config)
        tasks = []
        for symbol in symbols:
            save_to_db = (
                symbol not in self.ccxt[exchange]['symbols']
            )   # check db instead
            is_perp = strat_type != 'spot'
            symbol_tasks = await self.stream_symbol(
                exchange_name=exchange,
                symbol=symbol,
                timeframe=timeframe,
                pool=pool,
                is_perp=is_perp,
                save_to_db=save_to_db,
                routing_key=routing_key,
            )
            self.symbol_tasks[
                (strat_name, exchange, symbol, timeframe)
            ] = symbol_tasks
            for symbol_task in symbol_tasks:
                tasks.append(symbol_task)
        # self.ccxt[exchange]['symbols'].update(symbols)
        # self.ccxt_pro[exchange]['symbols'].update(symbols)
        asyncio.gather(*tasks)

    async def remove_symbols(self, strat_name):
        if strat_name in self.strats_data:
            exchange = self.strats_data[strat_name]['exchanges']
            symbols = self.strats_data[strat_name]['symbols']
            timeframe = self.strats_data[strat_name]['timeframe']
            for symbol in symbols:
                tasks = self.symbol_tasks.pop(
                    (strat_name, exchange, symbol, timeframe), None
                )
                if tasks:
                    for task in tasks:
                        task.cancel()
            del self.strats_data[strat_name]

    async def on_response(self, message):
        # Update HAL's state based on the message
        ...
        await message.ack()

    # -------------------------------------------
    #   DATABASE
    # -------------------------------------------

    ## DB utils

    async def connect_to_db(self):
        """
        Connect to a MySQL database with the given name.

        Parameters
        ----------

        Returns
        -------
        connection : mysql.connector.connection.MySQLConnection
            A connection to the specified database.
        """

        # connect to database
        try:
            self.pool = await aiomysql.create_pool(**self.db_config)
            self.conn = await self.pool.acquire()
            self.cursor = await self.conn.cursor()

            self.logger.info('Connection to db successful')

        except Exception as e:
            self.logger.error('Error while connecting to MySQL db %s', e)

    async def init_db(
        self,
    ):

        # Initialise db and create tables
        await self.connect_to_db()

        if self.backfill_window:
            await self.check_existing_tables()
            await self.backfill()

        else:
            # Create new tables
            await self.create_tables_market_data()
            await self.create_tables_trade_data()

    async def close_db(self):
        if self.pool is not None:
            await self.pool.close()
            await self.pool.wait_closed()
            self.pool = None

    async def get_db_pool(self):
        return await aiomysql.create_pool(**self.db_config)

    # -------------------------------------------
    # Create market data tables

    async def create_tables_market_data(self):
        """
        This function creates tables for storing market data in the database.
        It creates tables for exchange, trading symbol, and bar, tick, or orderbook data if specified.

        :param self: The instance of the class containing the necessary database connection and cursor.
        """

        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:

                    ## Create exchange table
                    query = """
                        CREATE TABLE IF NOT EXISTS exchange (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            name VARCHAR(100) NOT NULL UNIQUE
                        );
                    """
                    await cursor.execute(query)
                    await conn.commit()

                    ## Create symbol table
                    query = """
                        CREATE TABLE IF NOT EXISTS symbol (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            exchange_id INT,
                            symbol VARCHAR(100) NOT NULL,
                            base_asset VARCHAR(50) NOT NULL,
                            quote_asset VARCHAR(50) NOT NULL,
                            type VARCHAR(50) NOT NULL,
                            FOREIGN KEY (exchange_id) REFERENCES exchange(id),
                            UNIQUE KEY (exchange_id, symbol)
                        );
                    """

                    await cursor.execute(query)
                    await conn.commit()

                    await asyncio.gather(
                        *[
                            self.insert_exchange(exchange_name=exchange_name)
                            for exchange_name in self.exchanges
                        ]
                    )
                    await asyncio.gather(
                        *[
                            self.insert_symbols(exchange_name=exchange_name)
                            for exchange_name in self.exchanges
                        ]
                    )

                    if self.bar:
                        await self.create_table_bar_data()

                    if self.tick:
                        await self.create_table_tick_data()

                    if self.orderbook:
                        await self.create_table_orderbook_data()

                    if self.funding:
                        await self.create_table_funding_data()

                    self.logger.info('Market data tables created successfuly')
        except Exception as e:
            self.logger.error('Error while creating market data tables %s', e)

    async def create_table_funding_data(self):

        ## Create funding rate table
        await self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS funding_rate ( 
                id INT AUTO_INCREMENT PRIMARY KEY,
                exchange_id INT,
                symbol_id INT,
                timestamp BIGINT NOT NULL,
                rate DECIMAL(20, 8) NOT NULL,
                FOREIGN KEY (exchange_id) REFERENCES exchange(id),
                FOREIGN KEY (symbol_id) REFERENCES symbol(id),
                UNIQUE KEY (exchange_id, symbol_id, timestamp)
            );
        """
        )
        await self.conn.commit()

    async def create_table_bar_data(self):
        """
        Creates a table named 'bar_data' if it does not exist. This table is used to store
        bar/candlestick data from a cryptocurrency exchange. It includes columns for exchange
        ID, trading symbol ID, time period, timestamp, open, high, low, close prices, and volume.
        """

        await self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS bar (
                id INT PRIMARY KEY AUTO_INCREMENT,
                exchange_id INT,
                symbol_id INT,
                time_period ENUM('1m', '3m', '5m', '15m', '30m', '1h', '4h', '12h', '1d') NOT NULL,
                timestamp BIGINT NOT NULL,
                open_price DECIMAL(20, 8) NOT NULL,
                high_price DECIMAL(20, 8) NOT NULL,
                low_price DECIMAL(20, 8) NOT NULL,
                close_price DECIMAL(20, 8) NOT NULL,
                volume DECIMAL(20, 8) NOT NULL,
                FOREIGN KEY (exchange_id) REFERENCES exchange(id),
                FOREIGN KEY (symbol_id) REFERENCES symbol(id),
                UNIQUE KEY (exchange_id, symbol_id, time_period, timestamp)
            )
        """
        )
        await self.conn.commit()

    async def create_table_tick_data(self):
        """
        Creates a table named 'tick_data' if it does not exist. This table is used to store
        tick/trade data from a cryptocurrency exchange. It includes columns for exchange ID,
        trading symbol ID, trade ID, timestamp, price, volume, and trade side (buy/sell).
        """

        await self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS tick (
                id INT PRIMARY KEY AUTO_INCREMENT,
                exchange_id INT,
                symbol_id INT,
                trade_id VARCHAR(255) NOT NULL,
                timestamp BIGINT NOT NULL,
                price DECIMAL(20, 8) NOT NULL,
                volume DECIMAL(20, 8) NOT NULL,
                side ENUM('buy', 'sell') NOT NULL,
                FOREIGN KEY (exchange_id) REFERENCES exchange(id),
                FOREIGN KEY (symbol_id) REFERENCES symbol(id),
                UNIQUE KEY (exchange_id, symbol_id, trade_id)
            );
        """
        )
        await self.conn.commit()

    async def create_table_orderbook_data(self):
        """
        Creates tables for storing order book data if they do not exist. Three tables are created:
        'orderbook', 'bid', and 'ask'. 'orderbook' stores the snapshot of the
        order book with columns for exchange ID, trading symbol ID, and timestamp. The 'bid' and 'ask'
        tables store bid and ask data, respectively, with columns for orderbook_id, price,
        and volume. The 'bid' and 'ask' tables have a foreign key reference to the 'orderbook_snapshot' table.
        """

        # creata orderbook_snapshot table
        await self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS orderbook (
                id INT AUTO_INCREMENT PRIMARY KEY,
                exchange_id INT,
                symbol_id INT,
                timestamp BIGINT NOT NULL,
                FOREIGN KEY (exchange_id) REFERENCES exchange(id),
                FOREIGN KEY (symbol_id) REFERENCES symbol(id)
            );
        """
        )
        await self.conn.commit()

        # create table for storing bid data
        await self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS bid (
                id INT AUTO_INCREMENT PRIMARY KEY,
                orderbook_id INT,
                price DECIMAL(20, 8) NOT NULL,
                volume DECIMAL(20, 8) NOT NULL,
                FOREIGN KEY (orderbook_id) REFERENCES orderbook(id)
            );
        """
        )
        await self.conn.commit()

        # create table for storing ask data
        await self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS ask (
                id INT AUTO_INCREMENT PRIMARY KEY,
                orderbook_id INT,
                price DECIMAL(20, 8) NOT NULL,
                volume DECIMAL(20, 8) NOT NULL,
                FOREIGN KEY (orderbook_id) REFERENCES orderbook(id)
            );
        """
        )
        await self.conn.commit()

        # ---------------------------------------

    # -------------------------------------------
    # Create trade data tables

    async def create_tables_trade_data(self):

        await self.create_table_filled_trades()
        await self.create_table_open_orders()

        self.logger.info('Trade data tables created successfuly')

    async def create_table_filled_trades(self):

        await self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS filled (
                id INT AUTO_INCREMENT PRIMARY KEY,
                exchange_id INT,
                symbol_id INT,
                trade_id VARCHAR(255) NOT NULL,
                order_id VARCHAR(255) NOT NULL,
                timestamp BIGINT NOT NULL,
                price DECIMAL(20, 8) NOT NULL,
                volume DECIMAL(20, 8) NOT NULL,
                side ENUM('buy', 'sell') NOT NULL,
                fee DECIMAL(20, 8) NULL,
                fee_currency VARCHAR(50) NULL,
                FOREIGN KEY (exchange_id) REFERENCES exchange(id),
                FOREIGN KEY (symbol_id) REFERENCES symbol(id),
                UNIQUE KEY (exchange_id, symbol_id, trade_id)
            );
        """
        )
        await self.conn.commit()

    async def create_table_open_orders(self):

        ## Create open orders table
        query = """
            CREATE TABLE IF NOT EXISTS open_orders (
                id INT PRIMARY KEY AUTO_INCREMENT,
                exchange_id INT,
                symbol_id INT,
                order_id VARCHAR(255) NOT NULL,
                symbol VARCHAR(20) NOT NULL,
                type ENUM('market', 'limit', 'stop_loss', 'stop_loss_limit', 'take_profit', 'take_profit_limit') NOT NULL,
                side ENUM('buy', 'sell') NOT NULL,
                timestamp BIGINT NOT NULL,
                price DECIMAL(20, 8),
                amount DECIMAL(20, 8) NOT NULL,
                FOREIGN KEY (exchange_id) REFERENCES exchange(id),
                FOREIGN KEY (symbol_id) REFERENCES symbol(id),
                UNIQUE KEY (exchange_id, symbol_id, order_id)
            );
        """
        await self.cursor.execute(query)
        await self.conn.commit()

    # ---------------------------------------
    ## DB IDs

    async def get_exchange_id(self, cursor, exchange_name):
        try:
            select_exchange_query = 'SELECT id FROM exchange WHERE name = %s'
            await cursor.execute(select_exchange_query, (exchange_name,))
            exchange_id = (await cursor.fetchone())[0]
            return exchange_id
        except Exception as e:
            self.logger.error(
                'Error getting exchange ID for %s: %s', exchange_name, e
            )

    async def get_symbol_id(self, cursor, exchange_name, symbol):
        try:
            select_symbol_query = """
            SELECT symbol.id FROM symbol 
            INNER JOIN exchange ON symbol.exchange_id = exchange.id
            WHERE symbol.symbol = %s AND exchange.name = %s
            """
            await cursor.execute(select_symbol_query, (symbol, exchange_name))
            symbol_id_row = await cursor.fetchone()
            if symbol_id_row is None:
                self.logger.error(
                    f'No symbol found for {symbol} in exchange {exchange_name}'
                )
                return None
            else:
                symbol_id = symbol_id_row[0]
                return symbol_id
        except Exception as e:
            self.logger.error(
                'Error getting symbol ID for %s %s: %s',
                exchange_name,
                symbol,
                e,
            )

    # ----------------------------------------
    #   DATA INSERTION
    # ----------------------------------------

    async def insert_exchange(self, exchange_name):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                insert_query = 'INSERT IGNORE INTO exchange (name) VALUES (%s)'
                try:
                    await cursor.execute(insert_query, (exchange_name,))
                    await conn.commit()
                except Exception as e:
                    self.logger.error(
                        'Error writing %s exchange data to database: %s',
                        exchange_name,
                        e,
                    )

    async def insert_new_symbols(self, exchange_name, symbols, symbol_type):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                select_exchange_query = (
                    'SELECT id FROM exchange WHERE name = %s'
                )
                insert_symbol_query = 'INSERT IGNORE INTO symbol (exchange_id, symbol, base_asset, quote_asset, type) VALUES (%s, %s, %s, %s, %s)'

                await cursor.execute(select_exchange_query, (exchange_name,))
                exchange_id_row = await cursor.fetchone()
                exchange_id = exchange_id_row[0]

                # try:
                for symbol in symbols:
                    if ':' in symbol:
                        base_asset, quote_asset = symbol.split(':', 1)
                    else:
                        base_asset, quote_asset = symbol.split('/')
                    await cursor.execute(
                        insert_symbol_query,
                        (
                            exchange_id,
                            symbol,
                            base_asset,
                            quote_asset,
                            symbol_type,
                        ),
                    )
                await conn.commit()
                """ 
                except Exception as e:
                    self.logger.error(
                        'Error writing %s symbols to database: %s',
                        exchange_name,
                        e,
                    )
                """

    async def insert_symbols(self, exchange_name):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                select_exchange_query = (
                    'SELECT id FROM exchange WHERE name = %s'
                )
                insert_symbol_query = 'INSERT IGNORE INTO symbol (exchange_id, symbol, base_asset, quote_asset, type) VALUES (%s, %s, %s, %s, %s)'

                await cursor.execute(select_exchange_query, (exchange_name,))
                exchange_id_row = await cursor.fetchone()
                exchange_id = exchange_id_row[0]

                symbol_sources = [
                    (self.ccxt[exchange_name]['spot_symbols'], 'spot'),
                    (self.ccxt[exchange_name]['perps_symbols'], 'perps'),
                ]

                try:
                    for symbols, symbol_type in symbol_sources:
                        for symbol in symbols:
                            if ':' in symbol:
                                base_asset, quote_asset = symbol.split(':', 1)
                            else:
                                base_asset, quote_asset = symbol.split('/')
                            await cursor.execute(
                                insert_symbol_query,
                                (
                                    exchange_id,
                                    symbol,
                                    base_asset,
                                    quote_asset,
                                    symbol_type,
                                ),
                            )
                    await conn.commit()
                except Exception as e:
                    self.logger.error(
                        'Error writing %s symbols to database: %s',
                        exchange_name,
                        e,
                    )

    async def insert_open_order(self, conn, cursor, exchange_name, order):

        # Fetch exchange_id and pair_id for the symbol in the order
        # sql_fetch_ids = """SELECT exchange.id, pair.id FROM exchange, symbol
        #                    WHERE exchange.name = %s AND symbol.symbol = %s"""
        # await cursor.execute(sql_fetch_ids, (self.exchange_name, order['symbol']))

        try:
            exchange_id = await self.get_exchange_id(
                exchange_name=exchange_name, cursor=cursor
            )
            symbol_id = await self.get_symbol_id(
                cursor=cursor, symbol=order['symbol']
            )
            exchange_id, pair_id = await cursor.fetchone()

            # Insert the order data into the open_orders table
            sql_insert = """INSERT IGNORE INTO open_orders (exchange_id, symbol_id, order_id, symbol, type, side, timestamp, price, amount)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
            await cursor.execute(
                sql_insert,
                (
                    exchange_id,
                    symbol_id,
                    order['id'],
                    order['symbol'],
                    order['type'],
                    order['side'],
                    order['timestamp'],
                    order['price'],
                    order['amount'],
                ),
            )
            await conn.commit()

        except Exception as e:
            self.logger.error('Error storing open order: %s', e)

    async def insert_bar(
        self,
        conn,
        cursor,
        exchange_id,
        symbol_id,
        time_period,
        timestamp,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
    ):

        insert_query = """INSERT IGNORE INTO bar
                      (exchange_id, symbol_id, time_period, timestamp, open_price, high_price, low_price, close_price, volume)
                      VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        try:
            await cursor.execute(
                insert_query,
                (
                    exchange_id,
                    symbol_id,
                    time_period,
                    timestamp,
                    Decimal(open_price),
                    Decimal(high_price),
                    Decimal(low_price),
                    Decimal(close_price),
                    Decimal(volume),
                ),
            )
            await conn.commit()
        except Exception as e:
            self.logger.error(
                'Error writing %s bar data to database: %s', symbol_id, e
            )

    async def insert_bars(
        self,
        conn,
        cursor,
        bars_data,
    ):
        insert_query = """INSERT IGNORE INTO bar
                      (exchange_id, symbol_id, time_period, timestamp, open_price, high_price, low_price, close_price, volume)
                      VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        try:
            await cursor.executemany(
                insert_query,
                bars_data,
            )
            await conn.commit()
        except Exception as e:
            self.logger.error('Error writing bar data to database: %s', e)

    async def insert_funding(
        self, pool, exchange_id, symbol_id, funding_rate, funding_time
    ):
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                # Insert funding rate data
                insert_funding_rate_query = """
                    INSERT IGNORE INTO funding_rate (exchange_id, symbol_id, timestamp, rate)
                    VALUES (%s, %s, %s, %s)
                """
                await cursor.execute(
                    insert_funding_rate_query,
                    (
                        exchange_id,
                        symbol_id,
                        funding_time,
                        Decimal(funding_rate),
                    ),
                )
                await conn.commit()

    async def insert_trade(self, conn, cursor, trade_data):

        try:
            # ON DUPLICATE KEY UPDATE might not be needed?
            await cursor.execute(
                """
                INSERT IGNORE INTO filled (
                    exchange_id, symbol_id, trade_id, order_id, 
                    timestamp, price, volume, side, fee, fee_currency)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) AS new_values
                ON DUPLICATE KEY UPDATE
                    order_id = new_values.order_id,
                    timestamp = new_values.timestamp,
                    price = new_values.price,
                    volume = new_values.volume,
                    side = new_values.side,
                    fee = new_values.fee,
                    fee_currency = new_values.fee_currency;
                """,
                (
                    trade_data['exchange_id'],
                    trade_data['symbol_id'],
                    trade_data['trade_id'],
                    trade_data['order_id'],
                    trade_data['timestamp'],
                    Decimal(trade_data['price']),
                    Decimal(trade_data['volume']),
                    trade_data['side'],
                    Decimal(trade_data['fee']),
                    trade_data['fee_currency'],
                ),
            )
            await conn.commit()

        except Exception as e:
            self.logger.error('Error storing filled trade: %s', e)

    async def insert_trades(self, pool, trades, exchange_id, symbol_id):
        if not trades:
            return

        values = []
        for trade in trades:
            trade_data = (
                exchange_id,
                symbol_id,
                trade['id'],
                trade.get('order', None),
                trade['timestamp'],
                trade['price'],
                trade['amount'],
                trade['side'],
                trade['fee']['cost'] if trade.get('fee') else None,
                trade['fee']['currency'] if trade.get('fee') else None,
            )
            values.append(trade_data)

        sql = """
            INSERT IGNORE INTO filled (exchange_id, symbol_id, trade_id, order_id, timestamp, price, volume, side, fee, fee_currency)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.executemany(sql, values)
                await conn.commit()

    # -------------------------------------------
    #   DB MODIFICATION
    # -------------------------------------------

    async def update_order_in_db(self, conn, cursor, order):
        # Update the order data in the open_orders table
        sql_update = """UPDATE open_orders SET type = %s, side = %s, timestamp = %s, price = %s, amount = %s
                        WHERE order_id = %s AND symbol = %s"""
        try:
            await cursor.execute(
                sql_update,
                (
                    order['type'],
                    order['side'],
                    order['timestamp'],
                    order['price'],
                    order['amount'],
                    order['id'],
                    order['symbol'],
                ),
            )
            await conn.commit()

        except Exception as e:
            self.logger.error('Error updating order: %s', e)

    async def delete_canceled_orders_from_db(self, canceled_orders):
        try:
            async with aiomysql.connect(**self.db_config) as conn:
                async with conn.cursor() as cursor:
                    for order in canceled_orders:
                        order_id = order['id']
                        delete_query = (
                            'DELETE FROM open_orders WHERE order_id = %s'
                        )
                        await cursor.execute(delete_query, (order_id,))
                    await conn.commit()
        except Exception as e:
            self.logger.error('Error deleting canceled orders: %s', e)

    # -------------------------------------------
    #   DATA INPUT
    # -------------------------------------------

    # -------------------------------------------
    # Watch loops

    async def watch_trades_loop(
        self,
        exchange_name,
        symbol,
        pool,
        save_to_db: bool = True,
        publish: bool = False,
        routing_key: str = None,
    ):
        exchange_id, symbol_id = await self.get_ids(
            pool, exchange_name, symbol
        )
        while True:
            try:
                trades = await self.ccxt_pro[exchange_name][
                    'exchange'
                ].watch_my_trades(symbol)
                await self.process_trades(
                    pool,
                    trades,
                    exchange_id,
                    symbol_id,
                    save_to_db,
                    publish,
                    exchange_name,
                    symbol,
                    routing_key,
                )
            except ccxt.NotSupported:
                last_timestamp = None
                limit = 100

                while True:
                    trades = await self.fetch_new_trades(
                        exchange_name, symbol, last_timestamp, limit
                    )
                    if trades:
                        await self.process_trades(
                            pool,
                            trades,
                            exchange_id,
                            symbol_id,
                            save_to_db,
                            publish,
                            exchange_name,
                            symbol,
                            routing_key,
                        )
                    try:
                        last_timestamp = trades[-1]['timestamp']
                    except:
                        last_timestamp = None

                    await asyncio.sleep(5)

            except (
                ccxt.NetworkError,
                aiohttp.client_exceptions.ClientOSError,
            ) as e:
                await self.handle_network_error(e, exchange_name)
            except ccxt.ExchangeNotAvailable as e:
                self.logger.error(
                    f'Exchange not available error in watch_trades_loop: {e}'
                )
                continue
            except Exception as e:
                self.logger.error(
                    f'Unexpected error in watch_trades_loop: {e}'
                )
                continue

    async def watch_bar_loop(
        self,
        pool,
        exchange_name,
        symbol,
        timeframe,
        limit: int = 10,
        save_to_db: bool = True,
        publish: bool = False,
        routing_key: str = None,
    ):

        """
        We need to create a separate cursor for each concurrent operation.
        We do this by using a context manager for each query.
        """
        exchange_id, symbol_id = await self.get_ids(
            pool, exchange_name, symbol
        )

        last_timestamp = None

        while True:
            try:
                ohlcvs = await self.ccxt_pro[exchange_name][
                    'exchange'
                ].watch_ohlcv(symbol, timeframe, None, limit)
                current_timestamp = ohlcvs[-1][0]

                # Check if the current bar is closed
                current_time = self.ccxt_pro[exchange_name][
                    'exchange'
                ].milliseconds()
                current_bar_end_time = (
                    current_timestamp
                    + self.ccxt_pro[exchange_name]['exchange'].parse_timeframe(
                        timeframe
                    )
                    * 1000
                )
                is_closed = current_time >= current_bar_end_time

                if last_timestamp is None or (
                    current_timestamp > last_timestamp and is_closed
                ):
                    last_timestamp = current_timestamp

                    # Insert the bar data into the MySQL database
                    time_period = timeframe
                    bar = {
                        'timestamp': ohlcvs[-1][0],
                        'open_price': ohlcvs[-1][1],
                        'high_price': ohlcvs[-1][2],
                        'low_price': ohlcvs[-1][3],
                        'close_price': ohlcvs[-1][4],
                        'volume': ohlcvs[-1][5],
                    }

                    if save_to_db:
                        async with pool.acquire() as conn:
                            async with conn.cursor() as cursor:
                                await self.insert_bar(
                                    conn,
                                    cursor,
                                    exchange_id,
                                    symbol_id,
                                    time_period,
                                    bar['timestamp'],
                                    bar['open_price'],
                                    bar['high_price'],
                                    bar['low_price'],
                                    bar['close_price'],
                                    bar['volume'],
                                )

                        self.last_bar = ohlcvs[-1]
                    if publish:
                        message = {
                            'exchange': exchange_name,
                            'symbol': symbol,
                            'type': 'bar',
                            'bar': bar,
                        }
                        await self.publish_message(
                            message=message, routing_key=routing_key
                        )

            except (
                ccxt.NetworkError,
                aiohttp.client_exceptions.ClientOSError,
            ) as e:
                await self.handle_network_error(e, exchange_name)
            except ccxt.ExchangeNotAvailable as e:
                self.logger.error(f'Exchange not available error: {e}')
                pass
            except Exception as e:
                self.logger.error(
                    f'Unexpected error in watch_bar_loop: {e}, {type(e).__name__}'
                )
                continue

    async def watch_funding_rate(
        self,
        exchange_name,
        pool,
        symbol,
        save_to_db: bool = True,
        publish: bool = False,
        routing_key: str = None,
    ):
        exchange_id, symbol_id = await self.get_ids(
            pool, exchange_name, symbol
        )
        previous_timestamp = None

        while True:
            try:
                (
                    current_funding_rate,
                    current_timestamp,
                ) = await self.get_funding_rate(
                    exchange_name=exchange_name, symbol=symbol
                )

                if current_timestamp > (previous_timestamp or 0):
                    if save_to_db:
                        await self.insert_funding(
                            pool=pool,
                            exchange_id=exchange_id,
                            symbol_id=symbol_id,
                            funding_rate=current_funding_rate,
                            funding_time=current_timestamp,
                        )
                    if publish:
                        message = {
                            'exchange': exchange_name,
                            'symbol': symbol,
                            'type': 'funding',
                            'data': {
                                'funding_rate': current_funding_rate,
                                'funding_time': current_timestamp,
                            },
                        }
                        await self.publish_message(
                            message=message, routing_key=routing_key
                        )
                    previous_funding_rate = current_funding_rate
                    previous_timestamp = current_timestamp

                else:
                    await asyncio.sleep(60)

            except (
                ccxt.NetworkError,
                aiohttp.client_exceptions.ClientOSError,
            ) as e:
                await self.handle_network_error(e, exchange_name)
            except ccxt.ExchangeNotAvailable as e:
                self.logger.error(
                    f'Exchange not available error in watch_funding_rate: {e}'
                )
                continue
            except Exception as e:
                self.logger.error(
                    f'Unexpected error in watch_funding_rate: {e}'
                )
                continue

    # -------------------------------------------
    # Fetch data

    async def fetch_new_trades(
        self, exchange_name, symbol, last_timestamp, limit
    ):

        all_trades = []

        while True:
            try:
                fetched = await self.ccxt[exchange_name][
                    'exchange'
                ].fetch_my_trades(
                    symbol=symbol, since=last_timestamp, limit=limit
                )
                all_trades.extend(fetched)
                if len(fetched) < limit:
                    break
            except Exception as e:
                self.logger.error(
                    f'Unexpected error occurred while fetching trades: {e}'
                )
                await asyncio.sleep(5)
            continue

        return all_trades

    async def get_funding_rate(self, exchange_name, symbol):

        try:
            response = await self.ccxt[exchange_name][
                'exchange'
            ].fetch_funding_rate(symbol)

            funding_rate = response['fundingRate']
            funding_timestamp = response['fundingTimestamp']

            return funding_rate, int(funding_timestamp)

        except Exception as e:
            self.logger.error('Error getting funding rate: %s', e)

    async def fetch_bar(
        self,
        exchange_name,
        symbol,
        timeframe,
        window=None,
        timestamp_since=None,
    ):
        if window:
            ohlcv = await self.ccxt[exchange_name]['exchange'].fetch_ohlcv(
                symbol, timeframe, limit=window
            )
        elif timestamp_since:
            ohlcv = await self.ccxt[exchange_name]['exchange'].fetch_ohlcv(
                symbol, timeframe, since=timestamp_since
            )
        else:
            self.logger.error(
                f'Error: Neither window nor timestamp_since are present for {exchange_name} {symbol} {timeframe}'
            )
            return False
        return ohlcv

    async def fetch_and_insert_bar_data(
        self,
        conn,
        cursor,
        exchange_name,
        symbol,
        timeframe,
        window=None,
        timestamp_since=None,
    ):
        ohlcvs = await self.fetch_bar(
            exchange_name=exchange_name,
            symbol=symbol,
            timeframe=timeframe,
            window=window,
            timestamp_since=timestamp_since,
        )

        if not ohlcvs:
            self.logger.error(
                f"Couldn't backfill data for symbol {symbol} and exchange {exchange_name}"
            )

        symbol_id = await self.get_symbol_id(cursor, exchange_name, symbol)
        exchange_id = await self.get_exchange_id(cursor, exchange_name)

        bars_data = [
            (
                exchange_id,
                symbol_id,
                timeframe,
                ohlcv[0],
                ohlcv[1],
                ohlcv[2],
                ohlcv[3],
                ohlcv[4],
                ohlcv[5],
            )
            for ohlcv in ohlcvs
        ]

        await self.insert_bars(
            conn,
            cursor,
            bars_data,
        )

        return pd.DataFrame(
            ohlcvs,
            columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'],
        )
        # return ohlcv

    # ---------------------------------------
    # Data backfill

    async def fill_gap(self, pool):
        """
        This function is meant to be used to fill database gaps.
        """
        try:
            async with pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    for exchange_name in self.exchanges:
                        for symbol in self.ccxt[exchange_name]['symbols']:
                            for timeframe in self.timeframes:
                                query = 'SELECT MAX(timestamp) FROM bar WHERE symbol_id = %s AND exchange_id = %s AND time_period = %s'
                                params = (
                                    await self.get_symbol_id(
                                        cursor, exchange_name, symbol
                                    ),
                                    await self.get_exchange_id(
                                        cursor, exchange_name
                                    ),
                                    timeframe,
                                )
                                last_timestamp = (
                                    await self.execute_db_query(
                                        pool, query, params
                                    )
                                )[0]
                                self.backfill(timestamp_since=last_timestamp)
        except:
            self.logger.error('Error in gap backfill')

    async def backfill(self, pool, window=None, timestamp_since=None):
        """
        This function is meant to be used at the start of initializing a
        strategy to backfill data for symbols to track and enable the strategy to run
        or to fill gaps in the data.
        """

        try:
            async with pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    for exchange_name in self.exchanges:
                        for symbol in self.ccxt[exchange_name]['symbols']:
                            for timeframe in self.timeframes:
                                await self.fetch_and_insert_bar_data(
                                    conn,
                                    cursor,
                                    exchange_name,
                                    symbol,
                                    timeframe,
                                    window=window,
                                    timestamp_since=timestamp_since,
                                )
        except:
            self.logger.error('Error in backfill')

    async def symbol_backfill(
        self,
        symbols: str,
        exchange_name: str,
        timeframe: str,
        window: int,
        strat_name: str,
    ):
        tasks = []
        pool = await aiomysql.create_pool(**self.db_config)
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                for symbol in symbols:
                    tasks.append(
                        self.fetch_and_insert_bar_data(
                            conn,
                            cursor,
                            exchange_name,
                            symbol,
                            timeframe,
                            window,
                            strat_name,
                        )
                    )
                results = await asyncio.gather(*tasks)
        data = dict(zip(symbols, results))
        await self.publish_message(
            {
                'type': 'backfill_symbols',
                'data': {
                    k: v.to_json(orient='records') for k, v in data.items()
                },
                'symbols': symbols,
                'exchange_name': exchange_name,
                'timeframe': timeframe,
                'window': window,
            },
            strat_name,
        )

    # -------------------------------------------
    #   STREAM
    # -------------------------------------------

    async def stream_symbol(
        self,
        exchange_name,
        symbol,
        pool,
        timeframe,
        is_perp: bool = False,
        save_to_db: bool = False,
        publish: bool = True,
        routing_key: str = None,
    ):

        tasks = []

        # Create tasks for spot symbols
        tasks.append(
            self.watch_trades_loop(
                exchange_name=exchange_name,
                symbol=symbol,
                pool=pool,
                save_to_db=save_to_db,
                publish=publish,
                routing_key=routing_key,
            )
        )

        # Create tasks for bar
        if self.bar:
            tasks.append(
                self.watch_bar_loop(
                    exchange_name=exchange_name,
                    pool=pool,
                    symbol=symbol,
                    timeframe=timeframe,
                    save_to_db=save_to_db,
                    publish=publish,
                    routing_key=routing_key,
                )
            )

        # Create tasks for funding
        if self.funding and is_perp:
            tasks.append(
                self.watch_funding_rate(
                    exchange_name=exchange_name,
                    pool=pool,
                    symbol=symbol,
                    save_to_db=save_to_db,
                    publish=publish,
                    routing_key=routing_key,
                )
            )

        # Add tasks for orderbook and tick if needed
        if self.orderbook:
            pass  # Add task for orderbook

        if self.tick:
            pass  # Add task for tick

        return tasks

    async def stream(
        self,
    ):

        pools = [
            await aiomysql.create_pool(**self.db_config)
            for _ in range(len(self.exchanges))
        ]

        while True:

            try:

                all_tasks = []

                for idx, exchange_name in enumerate(self.exchanges):
                    pool = pools[idx]
                    spot_symbols = self.ccxt[exchange_name]['spot_symbols']
                    symbols = self.ccxt[exchange_name]['symbols']
                    perps_symbols = self.ccxt_pro[exchange_name][
                        'perps_symbols'
                    ]

                    tasks1 = [
                        self.watch_trades_loop(
                            exchange_name=exchange_name,
                            symbol=symbol,
                            pool=pool,
                        )
                        for symbol in symbols
                    ]

                    all_tasks.extend(tasks1)

                    if self.bar:
                        tasks2 = [
                            self.watch_bar_loop(
                                exchange_name=exchange_name,
                                pool=pool,
                                symbol=symbol,
                                timeframe=timeframe,
                            )
                            for symbol in symbols
                            for timeframe in self.timeframes
                        ]
                        all_tasks.extend(tasks2)

                    if self.funding:
                        tasks3 = [
                            self.watch_funding_rate(
                                exchange_name=exchange_name,
                                pool=pool,
                                symbol=symbol,
                            )
                            for symbol in perps_symbols
                        ]
                        all_tasks.extend(tasks3)

                    if self.orderbook:
                        pass

                    if self.tick:
                        pass

                await asyncio.gather(*all_tasks)

            except (
                ccxt.NetworkError,
                aiohttp.client_exceptions.ClientOSError,
            ) as e:
                self.logger.error('Connection error in stream: %s', e)
                continue

            except ccxt.ExchangeNotAvailable as e:
                self.logger.error(
                    'Exchange not available error in stream: %s', e
                )
                continue

            except Exception as e:
                self.logger.error('Unexpected error in stream: %s', e)
                continue

    # -------------------------------------------
    #   UTILS
    # -------------------------------------------

    async def restart_stream(self):
        # Stop the current stream
        self.is_running = False

        # Wait for a few seconds to ensure that all tasks have stopped
        await asyncio.sleep(5)

        # Start a new stream
        self.is_running = True
        tasks = [self.stream(), self.consume_messages()]
        await asyncio.gather(*tasks)

    async def handle_network_error(self, e, exchange_name):
        self.logger.error(f'Connection error: {e}')
        self.logger.info(f'Closing exchange {exchange_name} and reconnecting')

        await self.close_exchange(
            self.ccxt_pro[exchange_name]['exchange'],
            self.ccxt_pro[exchange_name]['exchange_name'],
        )
        await self.close_exchange(
            self.ccxt[exchange_name]['exchange'],
            self.ccxt[exchange_name]['exchange_name'],
        )

        await asyncio.sleep(5)

        await self.reconnect_exchange(exchange_name)

        await self.restart_stream()

    async def close_exchange(self, exchange, exchange_name, max_attempts=10):
        attempts = 0
        while attempts < max_attempts:
            try:
                await exchange.close()
                return  # Exit the loop if the exchange is closed successfully
            except Exception as e:
                attempts += 1
                if attempts == max_attempts:
                    raise Exception(
                        f'Failed to close the exchange {exchange_name} after {max_attempts} attempts: {e}'
                    )
                else:
                    await asyncio.sleep(
                        5
                    )  # Wait for a few seconds before retrying

    async def reconnect_exchange(self, exchange_name, max_attempts=5):
        for attempt in range(max_attempts):
            try:
                self.new_exchange(exchange_name=exchange_name)
                return  # Exit the loop if the reconnection is successful
            except Exception as e:
                if (
                    attempt < max_attempts - 1
                ):  # If this wasn't the last attempt
                    await asyncio.sleep(
                        5
                    )  # Wait for a few seconds before retrying
                else:  # If this was the last attempt
                    raise Exception(
                        f'Failed to reconnect to the exchange {exchange_name} after {max_attempts} attempts: {e}'
                    )

    async def is_symbol_tracked(self, symbol):
        pool = await aiomysql.create_pool(**self.db_config)
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                select_symbol_query = (
                    'SELECT symbol FROM symbol WHERE symbol = %s'
                )
                await cursor.execute(select_symbol_query, (symbol,))
                result = await cursor.fetchone()
                return result is not None

    async def is_exchange_tracked(self, exchange):
        pool = await aiomysql.create_pool(**self.db_config)
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                select_exchange_query = (
                    'SELECT name FROM exchange WHERE name = %s'
                )
                await cursor.execute(select_exchange_query, (exchange,))
                result = await cursor.fetchone()
                return result is not None

    async def check_existing_tables(self):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:

                if self.bar:
                    await cursor.execute("SHOW TABLES LIKE 'bar'")
                    if not await cursor.fetchone():
                        await self.create_table_bar_data()

                if self.funding:
                    await cursor.execute("SHOW TABLES LIKE 'funding_rate'")
                    if not await cursor.fetchone():
                        await self.create_table_funding_data()

                if self.tick:
                    await cursor.execute("SHOW TABLES LIKE 'tick'")
                    if not await cursor.fetchone():
                        await self.create_table_tick_data()

                if self.orderbook:
                    await cursor.execute("SHOW TABLES LIKE 'orderbook'")
                    if not await cursor.fetchone():
                        await self.create_table_orderbook_data()

    async def get_bar_history(
        self, symbol, exchange_name, timeframe, history_length
    ):
        # Connect to the MySQL database
        pool = await aiomysql.create_pool(**self.db_config)

        try:
            async with pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    # Get the symbol ID and exchange ID
                    symbol_id = await self.get_symbol_id(
                        cursor, exchange_name, symbol
                    )
                    exchange_id = await self.get_exchange_id(
                        cursor, exchange_name
                    )

                    # Query the bar table for the specified symbol, exchange, and timeframe
                    query = """
                    SELECT * FROM bar
                    WHERE symbol_id = %s AND exchange_id = %s AND time_period = %s
                    ORDER BY timestamp DESC
                    LIMIT %s
                    """
                    await cursor.execute(
                        query,
                        (symbol_id, exchange_id, timeframe, history_length),
                    )
                    rows = await cursor.fetchall()

                    # Create a list to store the bar data
                    bars = []

                    for row in rows:
                        bar = {
                            'exchange_id': row[0],
                            'symbol_id': row[1],
                            'time_period': row[2],
                            'timestamp': row[3],
                            'open_price': row[4],
                            'high_price': row[5],
                            'low_price': row[6],
                            'close_price': row[7],
                            'volume': row[8],
                        }

                        bars.append(bar)

                    # Create a dataframe from the list of bars
                    df = pd.DataFrame(bars)

                    return df

        finally:
            # Close the database connection
            pool.close()
            await pool.wait_closed()

    async def close_ccxt_connection(self):

        await asyncio.gather(
            *[
                self.ccxt[exchange_name]['exchange'].close()
                for exchange_name in self.exchanges
            ]
        )
        await asyncio.gather(
            *[
                self.ccxt_pro[exchange_name]['exchange'].close()
                for exchange_name in self.exchanges
            ]
        )
        pass

    async def is_connected(self):
        try:
            conn = await aiomysql.connect(**self.db_config)
            await conn.ping()
            conn.close()
            return True
        except Exception as e:
            self.logger.error('Error while connecting to MySQL db %s', e)
            return False

    async def get_ids(self, pool, exchange_name, symbol):
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                exchange_id = await self.get_exchange_id(
                    exchange_name=exchange_name, cursor=cursor
                )
                symbol_id = await self.get_symbol_id(
                    exchange_name=exchange_name,
                    cursor=cursor,
                    symbol=symbol,
                )
                return exchange_id, symbol_id

    async def process_trades(
        self,
        pool,
        trades,
        exchange_id,
        symbol_id,
        save_to_db,
        publish,
        exchange_name,
        symbol,
        routing_key,
    ):
        if trades:
            if save_to_db:
                await self.insert_trades(
                    pool=pool,
                    trades=trades,
                    exchange_id=exchange_id,
                    symbol_id=symbol_id,
                )
            if publish:
                message = {
                    'exchange': exchange_name,
                    'symbol': symbol,
                    'type': 'fill',
                    'data': trades,
                }
                await self.publish_message(
                    message=message, routing_key=routing_key
                )

    # -------------------------------------------
    #   RUN
    # -------------------------------------------

    async def run(self):

        self.is_running = True

        await self.init_db()
        await self.init_rmq()
        await self.purge_queue()   # purge message queue

        if not await self.is_connected():
            self.connect_to_db()

        while self.is_running:

            tasks = [self.stream(), self.consume_messages()]

            try:
                await asyncio.gather(*tasks)
            except (
                ccxt.NetworkError,
                aiohttp.client_exceptions.ClientOSError,
            ) as e:
                # Handle connection errors
                for exchange in self.exchanges:
                    await self.handle_network_error(
                        e=e, exchange_name=exchange
                    )
            except Exception as e:
                # Handle any exceptions that occur during streaming
                self.logger.error('Error in stream: %s', e)
                if self.reconnect_attempts < self.max_reconnect_attempts:
                    self.reconnect_attempts += 1
                    self.logger.info(
                        f'Reconnecting attempt {self.reconnect_attempts}...'
                    )
                    for exchange in self.exchanges:
                        await self.handle_network_error(
                            e=e, exchange_name=exchange
                        )
                else:
                    self.logger.error(
                        'Max reconnect attempts reached, stopping...'
                    )
                    self.is_running = False
            finally:
                # Close ccxt connection and database
                if not self.is_running:
                    await self.close_ccxt_connection()
                    await self.close_db()


# -------------------------------------------
# -------------------------------------------

hal = HAL()

hal.init(
    bar_data=True,
    orderbook_data=False,
    tick_data=False,
    funding_data=False,
    logger=logger,
    exchanges=['binance'],
    timeframes={'5m', '1h'},
)


async def main():

    await hal.run()


if __name__ == '__main__':
    asyncio.run(main())
