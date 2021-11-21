from binance.error import ParameterArgumentError
from binance.lib.utils import (
    check_required_parameter,
    check_type_parameter,
    convert_list_to_json_array,
)
from binance.lib.utils import check_required_parameters
import datetime
import pandas as pd

def ping(self):
    """Test Connectivity
    Test connectivity to the Rest API.

    GET /api/v3/ping

    https://binance-docs.github.io/apidocs/spot/en/#test-connectivity

    """

    url_path = "/api/v3/ping"
    return self.query(url_path)


def time(self):
    """Check Server Time
    Test connectivity to the Rest API and get the current server time.

    GET /api/v3/time

    https://binance-docs.github.io/apidocs/spot/en/#check-server-time

    """

    url_path = "/api/v3/time"
    return self.query(url_path)


def exchange_info(self, symbol: str = None, symbols: list = None):
    """Exchange Information
    Current exchange trading rules and symbol information

    GET /api/v3/exchangeinfo

    https://binance-docs.github.io/apidocs/spot/en/#exchange-information

     Args:
        symbol (str, optional): the trading pair
        symbols (list, optional): list of trading pairs
    """

    url_path = "/api/v3/exchangeInfo"
    if symbol and symbols:
        raise ParameterArgumentError("symbol and symbols cannot be sent together.")
    check_type_parameter(symbols, "symbols", list)
    params = {"symbol": symbol, "symbols": convert_list_to_json_array(symbols)}
    return self.query(url_path, params)


def depth(self, symbol: str, **kwargs):
    """Get orderbook.

    GET /api/v3/depth

    https://binance-docs.github.io/apidocs/spot/en/#order-book

    Args:
        symbol (str): the trading pair
    Keyword Args:
        limit (int, optional): limit the results. Default 100; valid limits:[5, 10, 20, 50, 100, 500, 1000, 5000]
    """

    check_required_parameter(symbol, "symbol")
    params = {"symbol": symbol, **kwargs}
    return self.query("/api/v3/depth", params)


def trades(self, symbol: str, **kwargs):
    """Recent Trades List
    Get recent trades (up to last 500).

    GET /api/v3/trades

    https://binance-docs.github.io/apidocs/spot/en/#recent-trades-list

    Args:
        symbol (str): the trading pair
    Keyword Args:
        limit (int, optional): limit the results. Default 500; max 1000.
    """
    check_required_parameter(symbol, "symbol")
    params = {"symbol": symbol, **kwargs}
    return self.query("/api/v3/trades", params)


def historical_trades(self, symbol: str, **kwargs):
    """Old Trade Lookup
    Get older market trades.

    GET /api/v3/historicalTrades

    https://binance-docs.github.io/apidocs/spot/en/#old-trade-lookup

    Args:
        symbol (str): the trading pair
    Keyword Args:
        limit (int, optional): limit the results. Default 500; max 1000.
        formId (int, optional): trade id to fetch from. Default gets most recent trades.
    """
    check_required_parameter(symbol, "symbol")
    params = {"symbol": symbol, **kwargs}
    return self.limit_request("GET", "/api/v3/historicalTrades", params)


def agg_trades(self, symbol: str, **kwargs):
    """Compressed/Aggregate Trades List

    GET /api/v3/aggTrades

    https://binance-docs.github.io/apidocs/spot/en/#compressed-aggregate-trades-list

    Args:
        symbol (str): the trading pair
    Keyword Args:
        limit (int, optional): limit the results. Default 500; max 1000.
        formId (int, optional): id to get aggregate trades from INCLUSIVE.
        startTime (int, optional): Timestamp in ms to get aggregate trades from INCLUSIVE.
        endTime (int, optional): Timestamp in ms to get aggregate trades until INCLUSIVE.
    """

    check_required_parameter(symbol, "symbol")
    params = {"symbol": symbol, **kwargs}
    return self.query("/api/v3/aggTrades", params)


def klines(self, symbol: str, interval: str, **kwargs):
    """Kline/Candlestick Data

    GET /api/v3/klines

    https://binance-docs.github.io/apidocs/spot/en/#kline-candlestick-data

    Args:
        symbol (str): the trading pair
        interval (str): the interval of kline, e.g 1m, 5m, 1h, 1d, etc.
    Keyword Args:
        limit (int, optional): limit the results. Default 500; max 1000.
        startTime (int, optional): Timestamp in ms to get aggregate trades from INCLUSIVE.
        endTime (int, optional): Timestamp in ms to get aggregate trades until INCLUSIVE.
    """
    check_required_parameters([[symbol, "symbol"], [interval, "interval"]])

    params = {"symbol": symbol, "interval": interval, **kwargs}
    return self.query("/api/v3/klines", params)


def second_to_mil(timestamp):
    return  str(int(timestamp) * 1000)


def get_data(self, symbol: str, interval: str, start: datetime.datetime, end: datetime.datetime):
    import time
    timeDic = {"10s": 10, "1m": 60, "5m": 300, "15m": 900, "30m": 1800, "1h": 3600, "4h": 4 * 3600,
                        "8h": 8 * 3600, "1d": 24 * 3600, "1w": 7 * 24 * 3600} 
    kReturnMax = 1000
    e = time.mktime(end.timetuple())
    s = time.mktime(end.timetuple())
    startTimeStamp = time.mktime(start.timetuple())
    seconds = timeDic[interval]
    dataList = []
    while s > startTimeStamp:
        num = min(int((e - startTimeStamp) / seconds), kReturnMax)
        num = max(1, num)  # if num is zero, then u cannot break this fucking loop
        s = e - seconds * num
        tmp = pd.DataFrame(self.klines(symbol, interval, startTime=second_to_mil(s), endTime=second_to_mil(e), limit=1000))  # okex timestamp need * 1000
        e = s
        dataList.append(tmp) # 最后一个是无用的
        time.sleep(0.1)  # 防止过于频繁
    c = pd.concat(dataList)
    c.drop_duplicates(inplace=True)
    try:
        c.columns = ["Open time", "open", "high", "low", "close", "Volume", \
                     "Close time", "Quote asset volume", "Number of trades", \
                         "Taker buy base asset volume", "Taker buy quote asset volume", "Ignore"]
    except:
        print(c)
        print(e - startTimeStamp)
    c = c.astype("float")
    # print(c.loc[1,"Open time"])
    c["time"] = (c["Open time"] / 1000).apply(BeiJingFromTimeStamp)
    # print(c.loc[1, "time"])
    c = c.set_index("time")
    c = c.sort_values(by="time")
    return c[c.columns[:-1]]


def BeiJingFromTimeStamp(timestamp, hours=0):
    dt = datetime.datetime.utcfromtimestamp(timestamp)
    dt = dt + datetime.timedelta(hours=hours)  # 中国默认时区 8
    return dt


def avg_price(self, symbol: str):
    """Current Average Price

    GET /api/v3/avgPrice

    https://binance-docs.github.io/apidocs/spot/en/#current-average-price

    Args:
        symbol (str): the trading pair
    """

    check_required_parameter(symbol, "symbol")
    params = {
        "symbol": symbol,
    }
    return self.query("/api/v3/avgPrice", params)


def ticker_24hr(self, symbol: str = None):
    """24hr Ticker Price Change Statistics

    GET /api/v3/ticker/24hr

    https://binance-docs.github.io/apidocs/spot/en/#24hr-ticker-price-change-statistics

    Args:
        symbol (str, optional): the trading pair
    """

    params = {
        "symbol": symbol,
    }
    return self.query("/api/v3/ticker/24hr", params)


def ticker_price(self, symbol: str = None):
    """Symbol Price Ticker

    GET /api/v3/ticker/price

    https://binance-docs.github.io/apidocs/spot/en/#symbol-price-ticker

    Args:
        symbol (str, optional): the trading pair
    """

    params = {
        "symbol": symbol,
    }
    return self.query("/api/v3/ticker/price", params)


def book_ticker(self, symbol: str = None):
    """Symbol Order Book Ticker

    GET /api/v3/ticker/bookTicker

    https://binance-docs.github.io/apidocs/spot/en/#symbol-order-book-ticker

    Args:
        symbol (str, optional): the trading pair
    """

    params = {
        "symbol": symbol,
    }
    return self.query("/api/v3/ticker/bookTicker", params)
