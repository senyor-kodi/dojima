CEX = {
    'binance': {
        'SPOT-SYMBOLS': ['ETH/USDT', 'SOL/USDT'],
        'PERPS-SYMBOLS': ['BTC/USDT:USDT', 'ETH/USDT:USDT', 'SOL/USDT:USDT'],
    },
    'okx': {
        'SPOT-SYMBOLS': ['BTC/USDT', 'ETH/USDT', 'SOL/USDT'],
        'PERPS-SYMBOLS': ['BTC/USDT:USDT', 'ETH/USDT:USDT', 'SOL/USDT:USDT'],
    },
    'woo': {
        'SPOT-SYMBOLS': ['BTC/USDT', 'ETH/USDT', 'SOL/USDT'],
        'PERPS-SYMBOLS': ['BTC/USDT:USDT', 'ETH/USDT:USDT', 'SOL/USDT:USDT'],
    },
}

CIRCUIT_BREAKERS = {
    'STABLECOINS': ['USDT', 'USDC', 'USD', 'DAI'],
    'MAX_RETRIES': 10,
    'INITIAL_REDUCTION': 0.995,
    'REDUCTION_STEP': 0.005,
    'MIN_REDUCTION': 0.9,
    'SELL_CHUNK_SIZE': 0.25,
    'TIMEOUT': 10,
    'BALANCE_CHANGE_THRESHOLDS': {
        '5m': -0.1,
        '1h': -0.2,
        'initial': -0.5,
    },
}
