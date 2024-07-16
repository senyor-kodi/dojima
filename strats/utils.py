import numpy as np
import pandas as pd
import mysql.connector
from mysql.connector import Error


def connect_to_db(pwd: str, db_name: str = 'binance_noodlin'):
    """
    Connect to a MySQL database with the given name.

    Parameters
    ----------
    db_name : str, optional
        The name of the database to connect to (default is 'binance_kline_1h').

    Returns
    -------
    connection : mysql.connector.connection.MySQLConnection
        A connection to the specified database.

    Raises
    ------
    Error
        If there is an error while connecting to the MySQL database.

    """

    # connect to database
    try:
        connection = mysql.connector.connect(
            host='localhost',
            user='root',
            password=pwd,
            database=f'{db_name}',
        )
        print(f'Connection to {db_name} successful')

    except Error as e:
        print('Error while connecting to MySQL', e)

    return connection


def get_all_table_names(cursor):
    """
    Retrieves the names of all tables in the current database using the
    provided database cursor.

    Args:
        cursor: A database cursor for executing SQL queries.

    Returns:
        A list of strings representing the names of all tables in the
        current database.
    """

    cursor.execute('SHOW TABLES')
    tables = [table[0] for table in cursor.fetchall()]
    return tables


# -------------------------------------------
# Indicators
# -------------------------------------------

# weighted and hull moving average computation functions
def WMA(s, period):
    """
    Weighted moving average calculation. Rolling moving average that gives
    arithmetically diminishing weights to past data


    Args:
        s (series): data to compute WMA for
        period (int): window of time to compute the WMA for

    Returns:
        series:
    """
    return s.rolling(period).apply(
        lambda x: ((np.arange(period) + 1) * x).sum()
        / (np.arange(period) + 1).sum(),
        raw=True,
    )


# exponential moving average
def EMA(s, period):
    """
    Exponential moving average calculation. Rolling moving average that gives
    exponentially diminishing weights to past data

    Args:
        s (series): data to compute WMA for
        period (int): window of time to compute the WMA for

    Returns:
        series: EMA results
    """
    return s.ewm(span=period, adjust=False).mean()


# hull moving average
def HMA(s, period):
    """
    Hull moving average calculation.

    Args:
        s (series): data to compute WMA for
        period (int): window of time to compute the WMA for

    Returns:
        series: HMA results
    """
    return WMA(
        WMA(s, period // 2).multiply(2).sub(WMA(s, period)),
        int(np.sqrt(period)),
    )


# exponential hull moving average
def EHMA(s, period):
    """
    Exponential Hull moving average calculation.

    Args:
        s (series): data to compute WMA for
        period (int): window of time to compute the WMA for

    Returns:
        series: EHMA results
    """
    return EMA(
        EMA(s, period // 2).multiply(2).sub(EMA(s, period)),
        int(np.sqrt(period)),
    )


def WMA_np(s, period):
    weights = np.arange(period, 0, -1)
    return np.dot(s[-period:], weights) / weights.sum()


def EMA_np(s, period):
    return s[-period:].ewm(span=period, adjust=False).mean()[-1]


def HMA_np(s, period):
    return WMA_np(
        WMA_np(s, period // 2) * 2 - WMA_np(s, period), int(np.sqrt(period))
    )
