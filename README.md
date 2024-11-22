# Dojima

Dojima is an event-driven algorithmic trading library built on Python. It is a modified version of [QTPyLib](https://qtpylib.io/docs/latest/), a trading library for traditional finance via Interactive Brokers. Dojima, however, is specifically tailored for cryptocurrency trading. The library streamlines backend processes, allowing users to focus primarily on developing and implementing trading strategies.

## Features

- Asynchronous, Event-Driven Architecture: Dojima uses asyncio for handling market data events and order execution, providing a non-blocking, highly concurrent architecture.
- Support for Multiple Exchanges: The library supports multiple exchanges via the ccxt library. It abstracts the details of connecting to different exchanges, allowing you to focus on your trading strategy.
- Data Handling: the library can handle different types of market data including ticks, bars, and order book data. By default, the data is saved to a MySQL database. The database tables are built on-start. 
- Error Handling, logging and debugging: the library includes robust error handling to ensure your trading bot can recover from unexpected events, comprehensive logging to help you debug your trading strategies.


### Main Differences to QTPyLib

Although Dojima's architecture is based on QTPyLib, it is a complete rewrite. Some of the main differences are:

- Focus on Cryptocurrency Trading: Unlike QTPyLib, which is built for traditional financial products, Dojima is specifically designed for cryptocurrency trading.
- Use of CCXT Library: Dojima uses the CCXT library to connect to crypto exchanges, as opposed to QTPyLib's use of [ezIbPy](https://github.com/ranaroussi/ezibpy#ezibpy-pythonic-wrapper-for-ibpy), a wrapper for [IbPy](https://github.com/blampe/IbPy#ibpy---interactive-brokers-python-api), a python API client for Interactive Brokers. 
- Use of Asyncio: Dojima uses the asyncio library for asynchronous operations, replacing QTPyLib's use of threading and multiprocessing.
- Use of aiomysql: Dojima uses aiomysql for handling connections to the MySQL database, as opposed to QTPyLib's use of pymysql.
- Use of [RabbitMQ](https://www.rabbitmq.com/): Dojima uses RabbitMQ, specifically the [aio-pika](https://github.com/mosquito/aio-pika) library for its asynchronous capabilities, instead of [ZMQ](https://zeromq.org/).
- Simplified Class Structure: Dojima combines the Broker and Algo classes from QTPyLib into a single Algo class for simplicity.


## Structure

There are 3 main components to the library: 

1. Hal: handles market data retrieval, processing and database I/O.
2. Algo: sends and processes orders/positions to/from exchanges, and communicates with Hal to pass data to strategies.
3. Strategies: subclass of Algo, which handles trading logic. 

While Hal and Algo are fully functional, their functionality can always be expanded and improved upon. An example strategy, an implementation of an exponential moving average (EMA) crossover, is provided under the strats folder. This strategy serves as a template for writing your own strategies.

## Requirements

- Python 3.8+
- MySQL Server
- RabbitMQ Server
- CCXT library
- aiomysql
- aio-pika

## To-do

- Incorporate backtesting and paper trading.
- Add a dashboard to track performance, positions, capital at risk, and other risk metrics.
- Add data handlers for tick and orderbook data. 

## Important Notice

Please exercise caution when using this library for live trading. It is recommended to thoroughly test and validate its performance before deploying any trading strategy in a live environment.

While the provided code is functional, only basic features have been rigorously tested. The library has not been subjected to extensive use in real-world trading scenarios over extended periods of time.

## Disclaimer

Trading in financial markets, including cryptocurrency markets, involves substantial risk and may not be suitable for all investors. The high degree of volatility can work against you as well as for you. Before deciding to trade, you should carefully consider your investment objectives, level of experience, and risk appetite. There is a possibility that you could sustain a loss of some or all of your initial investment and therefore you should not invest money that you cannot afford to lose. You should be aware of all the risks associated with financial market trading and seek advice from an independent financial advisor if you have any doubts.

The information and code provided in this library are for educational purposes only and do not constitute investment advice. The author of this library does not accept liability for any loss or damage, including without limitation to, any loss of profit, which may arise directly or indirectly from use of or reliance on such information or application of the code.

By using this library, you acknowledge and accept that all trading decisions are your own sole responsibility, and the author or anybody associated with this library cannot be held responsible for any losses that are incurred as a result. Your understanding and acceptance of this risk is greatly appreciated.
