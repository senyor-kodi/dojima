import pandas as pd
import numpy as np
import json
import asyncio
from rich import print

import ccxt.async_support as ccxt_async
import ccxt.pro

import sys

sys.path.append('/Users/danielroyo2227/kodama/kodama222/dojima/v5/')

from algo import Algo
from hal import HAL, hal
from strats.utils import EMA


class EMACross(Algo):
    def __init__(
        self,
        fast_period: int = 4,
        slow_period: int = 24,
        symbol: str = 'BTC/USDT',
        exchange_name: str = 'binance',
        timeframe: str = '5m',
        strat_type='spot',
        hal: HAL = hal,
        name='ema_cross',
    ):
        super().__init__()

        self.hal = hal

        self.name = name

        self.symbol = symbol

        self.fast_period = fast_period
        self.slow_period = slow_period

        self.position = 0
        self.last_signal = None

        self.backfill_window = self.slow_period * 2

        self.symbol = symbol
        self.base_asset = self.symbol.split('/')[0]
        self.quote_asset = self.symbol.split('/')[1]
        self.strat_symbols = set([self.symbol])

        self.strat_data = {
            'exchange': exchange_name,
            'symbols': self.strat_symbols,
            'timeframe': timeframe,
            'type': strat_type,
        }

        self.exchange_name = exchange_name
        self.strat_timeframe = timeframe

        self.df = pd.DataFrame()

        self.initialised = False

    async def on_start(self):
        await self.init_rabbitmq()
        self.consume_task = asyncio.create_task(self.consume_messages())
        await self.send_initialization_message()
        await self.send_backfill_message(
            symbols=self.symbol,
            window=self.backfill_window,
        )

    async def on_backfill_response(self, data):

        # self.df = pd.read_json(data['data'])
        self.df = {k: pd.read_json(v) for k, v in data['data'].items()}[
            self.symbol
        ]

        # Calculate HMA for the loaded data
        self.df['fast_hma'] = EMA(self.df['close'], self.fast_period)
        self.df['slow_hma'] = EMA(self.df['close'], self.slow_period)

        self.last_signal = (
            'buy'
            if self.df['fast_hma'].iloc[-1] > self.df['slow_hma'].iloc[-1]
            else 'sell'
        )
        if self.last_signal == 'buy':
            balance = await self.get_balances(
                coins=self.quote_asset, balance_type='free'
            )

            # Use get_quote method to fetch bid and ask prices
            _, bid, ask, _ = await self.get_quote(self.symbol)
            initial_increment = 0.001

            # Get maximum spot position allowed
            balance = await self.get_balances(
                coins=self.quote_asset, balance_type='free'
            )
            qty = balance[self.quote_asset] / ask

            order = {
                'symbol': self.symbol,
                'type': 'market',
                'side': 'buy',
                'amount': qty,
            }
            result = await self.send_order(order=order)
            if result:
                for fill in result['info']['fills']:
                    print(
                        f'Fill on {self.exchange_name} for {self.symbol} at price {fill["price"]} and amount {fill["qty"]}'
                    )

        self.initialised = True
        self.logger.info(f'Initialisation {self.name} completed')

    async def on_bar(self, data):

        if not self.initialised:
            return

        # Update data
        self.df = pd.concat(
            [self.df, pd.DataFrame([data['bar']])], ignore_index=True
        )

        # Resize dataframe if it exceeds self.max_window
        if len(self.df) > self.max_window:
            self.df = self.df[-self.max_window :]

        # Update HMA values
        self.df['fast_hma'] = EMA(self.df['close'], self.fast_period)
        self.df['slow_hma'] = EMA(self.df['close'], self.slow_period)

        current_signal = None
        if self.df['fast_hma'].iloc[-1] > self.df['slow_hma'].iloc[-1]:
            current_signal = 'buy'
        else:
            current_signal = 'sell'

        if current_signal != self.last_signal:
            print(f'New signal: {current_signal}')

            # Use get_quote method to fetch bid and ask prices
            _, bid, ask, _ = await self.get_quote(self.symbol)
            initial_increment = 0.001

            if current_signal == 'buy':
                # Get maximum BTCUSDT spot position allowed
                balance = await self.get_balances(
                    coins='USDT', balance_type='free'
                )
                qty = balance / ask * (1 + initial_increment)
                order_result = await self.place_order_with_increments(
                    symbol=self.symbol,
                    side='buy',
                    initial_price=ask,
                    amount=qty,
                )
            else:
                # Get balance of BTC, sell for USDT
                balance = await self.get_balances(
                    coins='BTC', balance_type='free'
                )
                qty = balance / bid * (1 - initial_increment)
                order_result = await self.place_order_with_increments(
                    symbol=self.symbol,
                    side='sell',
                    initial_price=bid,
                    amount=qty,
                )
        # Fetch and print balances
        base_balance = await self.get_balances(
            coins=self.base_asset, balance_type='free'
        )
        quote_balance = await self.get_balances(
            coins=self.quote_asset, balance_type='free'
        )
        self.logger.info(
            f'Base asset ({self.base_asset}) balance: {base_balance[self.base_asset]}'
        )
        self.logger.info(
            f'Quote asset ({self.quote_asset}) balance: {quote_balance[self.quote_asset]}'
        )

        self.last_signal = current_signal

    async def on_fill(self, data):
        # TODO
        # Update order in db to filled
        # Add risk metrics (equity at risk, etc)
        symbol = data.get('symbol')
        side = data['data'][0]['side']
        price = data['data'][0]['price']
        qty = data['data'][0]['amount']

        # Format data into a string
        fill_info = f'Fill info - Symbol: {symbol}, Side: {side}, Price: {price}, Quantity: {qty}'

        # Log the information
        self.logger.info(fill_info)

    async def on_quote(self):
        pass

    async def on_tick(self):
        pass

    async def on_funding_rate(self):
        pass

    async def on_orderbook(self):
        pass

    async def order_is_fully_filled(self, order):
        try:
            # Fetch latest trades from the exchange for this order
            trades = await self.exchange.fetch_order_trades(order['id'])

            # Calculate total amount traded from returned trades
            traded_amount = sum([trade['amount'] for trade in trades])

            # Compare traded amount vs original ordered amount
            if traded_amount >= order['amount']:
                return True   # Fully filled
            else:
                return order['amount'] - traded_amount   # Partially filled

        except ccxt.OrderNotFound:
            # Order closed but trades not found indicates it was fully filled
            return True

        except Exception as e:
            # Log any errors fetching trades or calculating fill amount
            self.logger.error('Error checking order filled amount: %s', e)
            return False

    # -------------------------------------------
    # -------------------------------------------

    async def run(self):
        try:
            while True:
                await self.consume_task
        except KeyboardInterrupt:
            print('Stopping strategy...')
            await self.stop()


# -------------------------------------------
# -------------------------------------------


strategy = EMACross()


async def main():

    await strategy.on_start()

    await strategy.run()


if __name__ == '__main__':
    asyncio.run(main())
