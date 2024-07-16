import pandas as pd
import numpy as np
import json
import time
import yaml
from rich import print

import ccxt.async_support as ccxt_async
import ccxt.pro

import asyncio
import aiomysql
import aio_pika

from hal import HAL

from config import CIRCUIT_BREAKERS

from decimal import Decimal, getcontext

# Set the precision and scale
getcontext().prec = 20
getcontext().Emax = 8

from abc import ABC, abstractmethod

from logger_config import logger


class Algo(ABC):
    def __init__(
        self,
        dbhost: str = '',
        dbport: str = None,
        dbname: str = '',
        dbuser: str = '',
        dbpass: str = '',
        exchange_name: str = '',
        strat_type: str = 'spot',
        strat_symbols: set = set(),
        timeframe: str = '1h',
        bar_data: bool = True,
        orderbook_data: bool = False,
        tick_data: bool = False,
        funding_data: bool = False,
        max_window: int = 1000,
        logger=logger,
        hal: HAL = None,
        strat_name: str = '',
    ) -> None:

        self.name = strat_name

        self.hal = hal

        self._lock = asyncio.Lock()

        self.strat_data = {
            'exchange': exchange_name,
            'symbols': strat_symbols,
            'timeframe': timeframe,
            'type': strat_type,
        }

        self.new_exchange()

        self.db_config = {
            'host': dbhost,
            'user': dbuser,
            'password': dbpass,
            'db': dbname,
        }

        self.bar = bar_data
        self.orderbook = orderbook_data
        self.tick = tick_data
        self.funding = funding_data

        self.circuit_breakers = CIRCUIT_BREAKERS

        self.max_window = max_window

        self.strat_timeframe = timeframe

        self.logger = logger

    # -------------------------------------------
    #   RabbitMQ
    # -------------------------------------------

    async def init_rabbitmq(self):
        self.rabbitmq_conn = await aio_pika.connect_robust(self.hal.rmq_url)
        self.rabbitmq_channel = await self.rabbitmq_conn.channel()
        self.rabbitmq_exchange = await self.rabbitmq_channel.get_exchange(
            self.hal.name
        )
        self.rabbitmq_queue = await self.rabbitmq_channel.declare_queue(
            exclusive=True
        )
        await self.rabbitmq_queue.bind(
            self.rabbitmq_exchange, routing_key=self.name
        )
        self.logger.info(f'RabbitMQ {self.name} strat and queue initialised')

    async def publish_message(self, message, routing_key):
        await self.rabbitmq_exchange.publish(
            aio_pika.Message(body=message.encode()), routing_key
        )

    async def consume_messages(self):
        try:
            while True:
                async with self.rabbitmq_queue.iterator() as queue_iter:
                    async for message in queue_iter:
                        asyncio.create_task(self.process_message(message))
        except KeyboardInterrupt:
            print('Stopping Algo...')
            self.is_running = False

    async def process_message(self, message):
        async with self._lock:
            try:
                if not message.channel.is_closed:
                    async with message.process():
                        data = json.loads(message.body.decode())
                        routing_key = message.routing_key
                        self.logger.info(
                            f"Received message of type {data['type']} from Hal name {routing_key}"
                        )
                        if 'type' in data:
                            if data['type'] == 'fill':
                                await self.on_fill(data)
                            elif data['type'] == 'quote':
                                await self.on_quote(data)
                            elif data['type'] == 'bar':
                                await self.on_bar(data)
                            elif data['type'] == 'tick':
                                await self.on_tick(data)
                            elif data['type'] == 'funding_rate':
                                await self.on_funding_rate(data)
                            elif data['type'] == 'orderbook':
                                await self.on_orderbook(data)
                            elif data['type'] == 'backfill_symbols':
                                await self.on_backfill_response(data)
                else:
                    self.logger.error(
                        'Channel is closed, cannot acknowledge message'
                    )
            except KeyboardInterrupt:
                print('Stopping Hal...')
                self.is_running = False

    async def send_initialization_message(self):
        message = json.dumps(
            {
                'action': 'add_symbols',
                'strat_name': self.name,
                'exchange': self.strat_data['exchange'],
                'symbols': list(self.strat_data['symbols']),
                'timeframe': self.strat_data['timeframe'],
                'strat_type': self.strat_data['type'],
            }
        )
        await self.publish_message(message, routing_key=self.hal.name)
        self.logger.info(f'Initialisation message sent')

    async def send_backfill_message(self, symbols, window):
        message = json.dumps(
            {
                'action': 'backfill',
                'strat_name': self.name,
                'exchange': self.strat_data['exchange'],
                'symbols': symbols,
                'timeframe': self.strat_data['timeframe'],
                #'strat_type': self.strat_data['type'], don't think I need this
                'window': window,
            }
        )
        await self.publish_message(message, routing_key=self.hal.name)
        self.logger.info(f'Backfill message sent')

    async def stop_tracking_symbols(self):
        message = json.dumps({'action': 'remove', 'strat_name': self.name})
        await self.publish_message(message, routing_key=self.hal.name)

    # -------------------------------------------
    #   BROKER
    # -------------------------------------------

    def new_exchange(self):

        # Read the YAML file
        with open('dojima/v3/sensitive_vars.yaml', 'r') as file:
            exchange_credentials = yaml.safe_load(file)

        # Access the credentials by exchange name
        api_key = exchange_credentials[self.strat_data['exchange']]['API-KEY']
        api_secret = exchange_credentials[self.strat_data['exchange']][
            'API-SECRET'
        ]

        exchange_params = {
            'apiKey': api_key,
            'secret': api_secret,
            'enableRateLimit': True,
            'options': {'keepAlive': True},
        }

        if self.strat_data['exchange'] == 'okx':
            exchange_params['password'] = exchange_credentials[
                self.strat_data['exchange']
            ]['PASSPHRASE']

        if self.strat_data['exchange'] == 'woo':
            exchange_params['uid'] = '17606'

        self.exchange = getattr(ccxt_async, self.strat_data['exchange'])(
            exchange_params
        )
        self.exchange_pro = getattr(ccxt.pro, self.strat_data['exchange'])(
            exchange_params
        )

    # -------------------------------------------
    # Get data

    async def get_balances(self, coins=None, balance_type=None):
        """Fetch account balance from exchange.

        Args:
            coins (str/list): Coin symbol or list of coin symbols to fetch balance for. If None, fetch all.
            balance_type (str): Balance type - 'free', 'used', or 'total'

        Returns:
            dict: Dict of {symbol: balance}
        """

        if balance_type is not None and balance_type not in (
            'free',
            'used',
            'total',
        ):
            self.logger.error('Invalid balance_type %s', balance_type)
            return

        balances = await self.exchange.fetch_balance()

        # Convert coins to list if it's a single string
        if coins and isinstance(coins, str):
            coins = [coins]

        # Get symbols either from the provided coins or from all available balances
        symbols = coins if coins else list(balances['total'].keys())

        result = {}
        for symbol in symbols:
            if symbol in balances:
                if balance_type:
                    result[symbol] = balances[symbol][balance_type]
                else:
                    result[symbol] = balances[symbol]
            else:
                self.logger.warning(
                    f'Coin symbol {symbol} not found in balance data.'
                )

        return result

    async def get_symbol_balance(self, symbol, balance_type=None):
        """Fetch balance for a specific symbol."""

        if balance_type is not None and balance_type not in (
            'free',
            'used',
            'total',
        ):
            self.logger.error('Invalid balance_type %s', balance_type)
            return

        try:
            if balance_type:
                return await self.exchange.fetch_balance()[symbol][
                    balance_type
                ]
            else:
                return await self.exchange.fetch_balance()[symbol]
        except KeyError:
            self.logger.error('%s balance not found', symbol)
            return

    async def update_positions(self):
        try:
            self.positions = await self.exchange.fetch_positions()
        except Exception as e:
            self.logger.error('Error fetching positions: %s', e)
            return None

    async def get_trade_by_symbol(self, symbol=None, params={}):
        try:
            if symbol:
                return await self.exchange.fetch_my_trades(
                    symbol=symbol, params=params
                )
            else:
                return await self.exchange.fetch_my_trades(params=params)
        except Exception as e:
            self.logger.error(
                'Error fetching trades in get_my_trades(): %s', e
            )
            return None

    async def get_trade_by_id(self, order_id):

        while True:
            try:
                trades = await self.exchange.fetch_order_trades(
                    order_id, symbol=None, since=None, limit=None, params={}
                )
                if trades:
                    return trades

                else:
                    await asyncio.sleep(5)

            except Exception as e:
                self.logger.error(
                    'Error fetching trade in get_trade_by_order_id(), order_id %s: %s',
                    order_id,
                    e,
                )
                return None

    async def get_funding_rate(self, symbol):

        try:
            response = await self.exchange.fetch_funding_rate(symbol)

            funding_rate = response['info']['fundingRate']
            funding_timestamp = response['info']['fundingTime']
            next_funding_time = response['info']['nextFundingTime']

        except Exception as e:
            self.logger.error('Error fetching funding rate: %s', e)
            return None

        return funding_rate, int(funding_timestamp), int(next_funding_time)

    async def get_quote(self, symbol):

        try:
            try:
                orderbook = await self.exchange.fetch_order_book(symbol)

            except ccxt.NetworkError as e:
                self.logger.error(
                    f'Network error fetching order book for {symbol}: {e}'
                )
                return None, None, None, None

            try:
                bid = (
                    orderbook['bids'][0][0]
                    if len(orderbook['bids']) > 0
                    else None
                )
                ask = (
                    orderbook['asks'][0][0]
                    if len(orderbook['asks']) > 0
                    else None
                )

            except (IndexError, TypeError) as e:
                self.logger.error(
                    f'Error extracting quote data for {symbol}: {e}'
                )
                return None, None, None, None

            quote = (bid + ask) / 2
            spread = ask - bid

            return quote, bid, ask, spread

        except Exception as e:
            self.logger.error(f'Error fetching quote for {symbol}: {e}')
            return None, None, None, None

    # -------------------------------------------
    # Not Implemented
    # TODO

    async def get_open_orders(
        self,
        symbol=None,
        since=None,
        limit=10,
        params={},
    ):
        pass

    async def udpate_order_history_in_db(
        self, symbol=None, since=None, limit=10, params={}
    ):
        pass

    # -------------------------------------------
    # Send

    async def send_order(self, order):

        try:
            result = await self.exchange.create_order(
                symbol=order['symbol'],
                type=order['type'],
                side=order['side'],
                amount=order['amount'],
                price=order['price'] if 'price' in order else None,
                params=order.get('params', {}),
            )
            self.logger.info(f'Order created: {result["info"]}')
            return result

        except Exception as e:
            self.logger.error('Error creating order %s', e)
            return None

    # Change to cancel and create new order instead of edit order
    async def modify_order(
        self, order_id, symbol, type, side, amount=None, price=None, params={}
    ):
        try:
            order = await self.exchange.edit_order(
                order_id, symbol, type, side, amount, price, params
            )
            self.logger.info(f'Order modified: {order}')

            async with aiomysql.connect(**self.db_config) as conn:
                async with conn.cursor() as cursor:
                    await self.hal.update_order_in_db(
                        conn, cursor, order=order
                    )
                conn.close()
            return order
        except Exception as e:
            self.logger.error('Error modifying order %s', e)
            return None

    async def cancel_order(
        self,
        order_ids: list = None,
        symbols: list = None,
        cancel_all: bool = False,
    ):
        canceled_orders = []

        if cancel_all:
            tasks = [
                cancel_order_helper(order['id'], symbol)
                for symbol in await self.exchange.load_markets()
                for order in await self.exchange.fetch_open_orders(symbol)
            ]
            await asyncio.gather(*tasks)
            await self.hal.delete_canceled_orders_from_db(canceled_orders)
            return

        async def cancel_order_helper(order_id, symbol):
            try:
                order = await self.exchange.cancel_order(order_id, symbol)
                canceled_orders.append(order)
            except ccxt.OrderNotFound:
                self.logger.error(
                    'Order %s not found for symbol %s', order_id, symbol
                )
            except ccxt.NetworkError as e:
                self.logger.error(
                    'Network error canceling order %s for symbol %s: %s',
                    order_id,
                    symbol,
                    e,
                )
            except ccxt.ExchangeError as e:
                self.logger.error(
                    'Exchange error canceling order %s for symbol %s: %s',
                    order_id,
                    symbol,
                    e,
                )
            except Exception as e:
                self.logger.error(
                    'Unknown error canceling order %s for symbol %s: %s',
                    order_id,
                    symbol,
                    e,
                )

        async def cancel_orders_for_symbols(symbols, order_ids):
            tasks = [
                cancel_order_helper(order_id, symbol)
                for symbol in symbols
                for order_id in order_ids
            ]
            await asyncio.gather(*tasks)

        if order_ids and symbols:
            await cancel_orders_for_symbols(symbols, order_ids)
        elif order_ids:
            await cancel_orders_for_symbols(
                self.exchange.load_markets(), order_ids
            )
        elif symbols:
            tasks = [
                cancel_order_helper(order['id'], symbol)
                for symbol in symbols
                for order in await self.exchange.fetch_open_orders(symbol)
            ]
            await asyncio.gather(*tasks)

        await self.hal.delete_canceled_orders_from_db(canceled_orders)

    async def place_order_with_increments(
        self,
        symbol,
        side,
        initial_price,
        amount,
        increment_step=0.001,
        max_increment=0.01,
        max_time=60,
    ):
        start_time = time.time()
        current_increment = increment_step
        remaining_amount = amount

        while current_increment <= max_increment:
            # Check if max_time has been reached
            elapsed_time = time.time() - start_time
            if elapsed_time > max_time:
                self.logger.error(
                    f'Max time of {max_time}s reached. Exiting loop.'
                )
                break

            if side == 'buy':
                price = initial_price * (1 + current_increment)
            elif side == 'sell':
                price = initial_price * (1 - current_increment)
            else:
                self.logger.error(f'Invalid order side: {side}')
                return

            order = {
                'symbol': symbol,
                'type': 'limit',
                'side': side,
                'amount': remaining_amount,
                'price': price,
            }
            result = await self.send_order(order)

            filled_status = await self.order_is_fully_filled(result)

            if filled_status is True:  # Fully filled
                self.logger.info(
                    f'Successful {side} order on {self.strat_data["exchange"]} for {symbol} at price {result[price]} and amount {result[amount]}'
                )
                return result
            elif (
                filled_status is not False
            ):  # Partially filled, filled_status contains the remaining amount
                self.logger.info(
                    f'Partially filled {side} order on {self.strat_data["exchange"]} for {symbol} at price {result[price]} and amount {result[amount]}'
                )

                # Cancel existing order for remainder
                await self.cancel_order(order_ids=[result['id']])

                remaining_amount = filled_status  # Update remaining amount

                # Fetch updated bid and ask prices
                quote, bid, ask, spread = await self.get_quote(symbol)

                # Create new order at best bid/ask plus/minus 0.005
                new_price = bid + 0.005 if side == 'buy' else ask - 0.005

                new_order = {
                    'symbol': symbol,
                    'type': 'limit',
                    'side': side,
                    'amount': remaining_amount,
                    'price': new_price,
                }
                new_result = await self.send_order(new_order)

                new_filled_status = await self.order_is_fully_filled(
                    new_result
                )

                if new_filled_status is True:  # Fully filled
                    self.logger.info(
                        f'Successful {side} order on {self.strat_data["exchange"]} for {symbol} at price {result[price]} and amount {result[amount]}'
                    )
                    return new_result
                elif (
                    new_filled_status is not False
                ):  # Partially filled again, log an error
                    self.logger.info(
                        f'Partially filled {side} order on {self.strat_data["exchange"]} for {symbol} at price {result[price]} and amount {result[amount]}'
                    )
                    return None

            current_increment += increment_step

        # Switch to market order for selling if increment limit reached
        order = {
            'symbol': symbol,
            'type': 'market',
            'side': side,
            'amount': remaining_amount,
        }
        result = await self.send_order(order)
        self.logger.warning(
            f'Switched to market order for selling remaining position on {symbol}. Successful {side} order on {self.strat_data["exchange"]} for {symbol} at price {result[price]} and amount {result[amount]}'
        )

        return result

    async def halt_and_catch_fire(self):

        balances = await self.get_balances()

        # Filter out stablecoins
        non_stable_balances = {
            k: v
            for k, v in balances.items()
            if k not in self.circuit_breakers['STABLECOINS']
        }

        for coin, balance in non_stable_balances.items():
            try:
                await asyncio.wait_for(
                    self.fire_sell(coin, balance),
                    self.circuit_breakers['TIMEOUT'],
                )
            except asyncio.TimeoutError:
                self.logger.error(
                    f'{coin} took too long to sell! Moving on to the next coin.'
                )
                continue

        # Close connections (assuming you have a method to do this)
        await self.close_connections()
        self.logger.info('Emergency sell completed!')

    async def fire_sell(self, coin, balance):

        # Iterate over all stablecoins and try to sell into each sequentially
        for stablecoin in self.circuit_breakers['STABLECOINS']:
            bid = None
            try:
                bid = await self.get_quote(f'{coin}/{stablecoin}')
            except:
                continue

            if bid is None:
                # Log the error and continue to the next stablecoin
                self.logger.error(
                    f'Error: No bid found for {coin}/{stablecoin} pair.'
                )
                continue

            chunks = int(balance / self.circuit_breakers['SELL_CHUNK_SIZE'])

            for i in range(chunks):

                amount = balance * self.circuit_breakers['SELL_CHUNK_SIZE']

                price = bid * self.circuit_breakers['INITIAL_REDUCTION']

                while price >= bid * self.circuit_breakers['MIN_REDUCTION']:

                    order = {
                        'symbol': f'{coin}/{stablecoin}',
                        'type': 'limit',
                        'side': 'sell',
                        'amount': amount,
                        'price': price,
                    }

                    try:
                        await self.send_order(order)
                        break

                    except:
                        price -= bid * self.circuit_breakers['REDUCTION_STEP']

                if price < bid * self.circuit_breakers['MIN_REDUCTION']:
                    self.logger.error(
                        f'Failed to sell {amount} {coin} at {stablecoin} even at {price} price'
                    )
                    break

    # -------------------------------------------
    #   Abstract Methods
    # -------------------------------------------

    # -------------------------------------------
    # On start

    @abstractmethod
    def on_start(self):
        """
        Invoked once when algo starts. Used for when the strategy
        needs to initialize parameters upon starting.

        """
        # raise NotImplementedError("Should implement on_start()")
        pass

    # -------------------------------------------
    # On data received

    @abstractmethod
    async def on_backfill_response(self, *args, **kwargs):
        pass

    @abstractmethod
    async def on_fill(self, *args, **kwargs):
        pass

    @abstractmethod
    async def on_quote(self, *args, **kwargs):
        pass

    @abstractmethod
    async def on_bar(self, *args, **kwargs):
        pass

    @abstractmethod
    async def on_tick(self, *args, **kwargs):
        pass

    @abstractmethod
    async def on_funding_rate(self, *args, **kwargs):
        pass

    @abstractmethod
    async def on_orderbook(self, *args, **kwargs):
        pass

    # -------------------------------------------

    async def log_trade(self, trade):
        pass

    async def halt_and_catch_fire(self):

        balances = await self.get_balances()

        # Filter out stablecoins
        non_stable_balances = {
            k: v
            for k, v in balances.items()
            if k not in self.circuit_breakers['STABLECOINS']
        }

        for coin, balance in non_stable_balances.items():
            try:
                await asyncio.wait_for(
                    self.fire_sell(coin, balance),
                    self.circuit_breakers['TIMEOUT'],
                )
            except asyncio.TimeoutError:
                self.logger.error(
                    f'{coin} took too long to sell! Moving on to the next coin.'
                )
                continue

        # Close connections (assuming you have a method to do this)
        await self.close_connections()
        self.logger.info('Emergency sell completed!')

    async def fire_sell(self, coin, balance):

        # Iterate over all stablecoins and try to sell into each sequentially
        for stablecoin in self.circuit_breakers['STABLECOINS']:
            bid = None
            try:
                bid = await self.get_quote(f'{coin}/{stablecoin}')
            except:
                continue

            if bid is None:
                # Log the error and continue to the next stablecoin
                self.logger.error(
                    f'Error: No bid found for {coin}/{stablecoin} pair.'
                )
                continue

            chunks = int(balance / self.circuit_breakers['SELL_CHUNK_SIZE'])

            for i in range(chunks):

                amount = balance * self.circuit_breakers['SELL_CHUNK_SIZE']

                price = bid * self.circuit_breakers['INITIAL_REDUCTION']

                while price >= bid * self.circuit_breakers['MIN_REDUCTION']:

                    order = {
                        'symbol': f'{coin}/{stablecoin}',
                        'type': 'limit',
                        'side': 'sell',
                        'amount': amount,
                        'price': price,
                    }

                    try:
                        await self.send_order(order)
                        break

                    except:
                        price -= bid * self.circuit_breakers['REDUCTION_STEP']

                if price < bid * self.circuit_breakers['MIN_REDUCTION']:
                    self.logger.error(
                        f'Failed to sell {amount} {coin} at {stablecoin} even at {price} price'
                    )
                    break

    async def check_account_balance(self):

        initial_balance = await self.get_initial_balance()

        self.balance_5min_ago = initial_balance
        self.balance_1hr_ago = initial_balance

        while True:

            current_balance = await self.get_current_balance()

            pct_change_5min = (
                current_balance - self.balance_5min_ago
            ) / self.balance_5min_ago
            pct_change_1hr = (
                current_balance - self.balance_1hr_ago
            ) / self.balance_1hr_ago
            pct_change_total = (
                current_balance - initial_balance
            ) / initial_balance

            if pct_change_5min <= -0.1:
                self.halt_and_catch_fire()

            if pct_change_1hr <= -20:
                self.halt_and_catch_fire()

            if pct_change_total <= -50:
                self.halt_and_catch_fire()

            # Update previous balances
            self.balance_5min_ago = current_balance
            self.balance_1hr_ago = current_balance

            await asyncio.sleep(300)

    # -------------------------------------------
    # DATABASE
    # -------------------------------------------

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

            self.logger.info('Connection successful')

        except Exception as e:
            self.logger.error('Error while connecting to MySQL db %s', e)

    # -------------------------------------------

    async def init_db(
        self,
    ):

        # Initialise db and create tables
        await self.connect_to_db()
        await self.add_exchange()
        await self.add_symbols()
