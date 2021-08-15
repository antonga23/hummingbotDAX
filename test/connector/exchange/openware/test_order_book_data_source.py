import hashlib
import hmac
import requests
import time
import urllib

import asyncio
import aiohttp
import logging
import pandas as pd
from typing import (
    Any,
    AsyncIterable,
    Dict,
    List,
    Optional,
    Tuple
)
import time
import ujson
import websockets
from websockets.exceptions import ConnectionClosed
from decimal import Decimal
import re

from binance.client import Client as BinanceClient

openware_api_url = 'https://www.quantaexchange.org/api/v2/peatio'
openware_ranger_url = 'https://www.quantaexchange.org/api/v2/ranger'

tradingPair = "ethusd"

async def safe_gather(*args, **kwargs):
    try:
        return await asyncio.gather(*args, **kwargs)
    except Exception as e:
        print(f"Unhandled error in background task: {str(e)}")
        raise
  
# https://www.quantaexchange.org/api/v2/peatio/public/markets/ethusd/depth
async def get_snapshot(client: aiohttp.ClientSession, trading_pair: str, limit: int = 1000) -> Dict[str, Any]:
    params: Dict = {"limit": str(limit)} if limit != 0 else {}
    #openware_api_url = "https://www.quantaexchange.org/api/v2/peatio"
    uri = "%s/public/markets/%s/depth" % (openware_api_url, trading_pair)
    async with client.get(uri, params=params) as response:
        response: aiohttp.ClientResponse = response
        if response.status != 200:
            raise IOError(f"Error fetching Openware market snapshot for {trading_pair}. "
                          f"HTTP status is {response.status}.")
        data: Dict[str, Any] = await response.json()

        # Need to add the symbol into the snapshot message for the Kafka message queue.
        # Because otherwise, there'd be no way for the receiver to know which market the
        # snapshot belongs to.

        return data

async def get_active_exchange_markets() -> pd.DataFrame:
    """
    Returned data frame should have trading_pair as index and include usd volume, baseAsset and quoteAsset
    """
    async with aiohttp.ClientSession() as client:
        #openware_api_url = "https://www.quantaexchange.org/api/v2/peatio"
        uri_market = "%s/public/markets" % openware_api_url
        uri_ticker = "%s/public/markets/tickers" % openware_api_url
        market_response, exchange_response = await safe_gather(
            client.get(uri_market),
            client.get(uri_ticker)
        )

        if market_response.status != 200:
            raise IOError(f"Error fetching Openware markets information. "
                          f"HTTP status is {market_response.status}.")
        if exchange_response.status != 200:
            raise IOError(f"Error fetching Openware exchange information. "
                          f"HTTP status is {exchange_response.status}.")
        
        market_data = await market_response.json()
        exchange_data = await exchange_response.json()
        
        trading_pairs: Dict[str, Any] = {item["id"]: {k: item[k] for k in ["base_unit", "quote_unit"]}
                                         for item in market_data}

        market_data: List[Dict[str, Any]] = [{**item, **exchange_data[item["id"]]["ticker"]}
                                             for item in market_data]

        # Build the data frame.
        all_markets: pd.DataFrame = pd.DataFrame.from_records(data=market_data, index="id")

        return all_markets.sort_values("last", ascending=False)
    
async def get_trading_pairs() -> List[str]:
    try:
        active_markets: pd.DataFrame = await get_active_exchange_markets()
        _trading_pairs = active_markets.index.tolist()
    except Exception:
        _trading_pairs = []
        print("Error getting active exchange information. Check network connection.")
    return _trading_pairs

def convert_to_exchange_trading_pair(hb_trading_pair: str) -> str:
    # Openware does not split 'basequote' (btcusdt) and must be lowercase.
    return hb_trading_pair.replace("-", "").lower()




def split_trading_pair(trading_pair: str) -> Optional[Tuple[str, str]]:
#    return trading_pair.replace('usdc', ''), 'usdc'
    #try:
    #tpstring = global_config_map.get("trading_pair_splitter").value.lower()
    tpstring = "ETH|EUR|USD".lower()
    trading_pair_splitter = re.compile(rf"^(\w+)({tpstring})$")
    m = trading_pair_splitter.match(trading_pair.lower())
    return m.group(1), m.group(2)
    #except Exception as e:
        #return None


def convert_from_exchange_trading_pair(exchange_trading_pair: str) -> Optional[str]:
    if split_trading_pair(exchange_trading_pair) is None:
        return None
    # Openware does not split BASEQUOTE (BTCUSDT)
    base_asset, quote_asset = split_trading_pair(exchange_trading_pair)
    return f"{base_asset.upper()}-{quote_asset.upper()}"





async def fetch_trading_pairs(domain="com") -> List[str]:
    try:
        raw_trading_pairs = await get_trading_pairs()
        trading_pair_list: List[str] = []
        for raw_trading_pair in raw_trading_pairs:
            converted_trading_pair: Optional[str] = convert_from_exchange_trading_pair(raw_trading_pair)
            if converted_trading_pair is not None:
                trading_pair_list.append(converted_trading_pair)
        return trading_pair_list
    except Exception as e:
        #print(str(e))
        # Do nothing if the request fails -- there will be no autocomplete for openware trading pairs
        pass
    return []





async def get_last_traded_price(trading_pair: str, domain: str = "com") -> float:
    #openware_api_url = "https://www.quantaexchange.org/api/v2/peatio"
    async with aiohttp.ClientSession() as client:
        url = openware_api_url
        symbol = convert_to_exchange_trading_pair(trading_pair)
        url = f"{url}/public/markets/{symbol}/tickers"
        resp = await client.get(url)
        resp_json = await resp.json()
        return float(resp_json['ticker']['last'])


async def ping_server(client: aiohttp.ClientSession) -> Dict[str, Any]:
    params: Dict = {"limit": 1}
    #openware_api_url = "https://www.quantaexchange.org/api/v2/peatio"
    uri = f"{openware_api_url}/public/health/alive"
    async with client.get(uri, params=params) as response:
        response: aiohttp.ClientResponse = response
        if response.status != 200:
            raise IOError("Error pinging Openware server might be down. Check connection or API KEYS and try again.")
        data = await response.text()
        if data == "200":
            return {}
        else:
            raise IOError("Error pinging Openware server might be down. Check connection or API KEYS and try again.")

async def test_snapshot():
    print("Test openware get market depth snapshot")
    async with aiohttp.ClientSession() as client:
        result = await get_snapshot(client, "ethusd")
        assert len(result["bids"]) >= 1
        assert len(result["asks"]) >= 1
        print(result)
        
async def test_get_trading_pairs():
    global result
    print("Test openware get trading pairs")
    result = await get_trading_pairs()
    print(result)

async def test_get_last_price():
    lastPrice = await get_last_traded_price("ETH-USD","com")
    print("last price: " + str(lastPrice))
    
async def test_get_all_mid_prices():
    result = await get_all_mid_prices("com")
    print(result)

async def test_get_fetch_trading_pairs():
    result = await fetch_trading_pairs("com")
    print(result)

async def test_ping_server():
    print("Test openware ping server")
    async with aiohttp.ClientSession() as client:
        result = await ping_server(client)
        print(result)


"""
#_throttler = Throttler((10.0, 1.0))
async def query_api(func, *args, app_warning_msg: str = "Openware API call failed. Check API key and network connection.", request_weight: int = 1, **kwargs) -> Dict[str, any]:
    async with _throttler.weighted_task(request_weight=request_weight):
        try:
            return await _async_scheduler.call_async(partial(func, *args, **kwargs), timeout_seconds=API_CALL_TIMEOUT, app_warning_msg=app_warning_msg)
        except Exception as ex:
            if "Timestamp for this request" in str(ex):
                print("Got Openware timestamp error. Need to force update Openware server time offset...")
            raise ex
"""


        
def _init_session():

    timestamp = str(time.time() * 1000)
    signature = _generate_signature(timestamp)
    session = requests.session()
    session.headers.update({'Accept': 'application/json',
                            'User-Agent': 'openware/python',
                            'X-Auth-Apikey': api_key,
                            'X-Auth-Nonce': timestamp,
                            'X-Auth-Signature': signature})
    return session

def update_headers():
    
    timestamp = str(time.time() * 1000)
    signature = _generate_signature(timestamp)
    session.headers.update({'Accept': 'application/json',
                            'User-Agent': 'openware/python',
                            'X-Auth-Apikey': api_key,
                            'X-Auth-Nonce': timestamp,
                            'X-Auth-Signature': signature})
    return session

def _create_api_uri(path):
    return "%s%s" % (api_url, path)

def _generate_signature(timestamp):
    query_string = "%s%s" % (timestamp, api_key)
    m = hmac.new(api_secret.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256)
    return m.hexdigest()

def _request(method, uri, force_params=False, **kwargs):

    data = kwargs.get('data', None)
    if data and isinstance(data, dict):
        kwargs['data'] = data
    # if get request assign data array to params value for requests lib
    if data and (method == 'get' or force_params):
        kwargs['params'] = kwargs['data']
        del(kwargs['data'])
    update_headers()
    response = getattr(session, method)(uri, **kwargs)
    return _handle_response(response)

def _request_api(method, path, **kwargs):
    uri = _create_api_uri(path)
    #print(method, uri, **kwargs) # PRINTY
    return _request(method, uri, **kwargs)

def _handle_response(response):
    
    if not str(response.status_code).startswith('2'):
        raise print("OpenwareAPIException: ", response)
    try:
        resp = response.json()
        return resp
    except ValueError:
        print('Invalid Response: %s' % response.text)

def _get( path, **kwargs):
    return _request_api('get', path, **kwargs)

async def get_my_trades(**params):
    print("get_my_trades() ??????")
    result = _get("/market/trades", data=params)
    print("get_my_trades() ------", result) # PRINTY
    return result


api_key = "9d833da4f5651cc4"
api_secret = "f9b5a592b79cc73e712d99179b246128"
api_url = "https://www.quantaexchange.org/api/v2/peatio"
session = _init_session()

_last_poll_timestamp = 0
_current_timestamp = 0
async def _history_reconciliation():
    LONG_POLL_INTERVAL = 120.0
    
    # Method looks in the exchange history to check for any missing trade in local history.
    # If found, it will trigger an order_filled event to record it in local DB.
    # The minimum poll interval for order status is 120 seconds.
    last_tick = (_last_poll_timestamp / LONG_POLL_INTERVAL)
    current_tick = (_current_timestamp / LONG_POLL_INTERVAL)
    current_tick = 5
    last_tick = 0
    if current_tick > last_tick:
        trading_pairs = ['ETH-USD']
        #trading_pairs = self._order_book_tracker._trading_pairs
        #tasks = [query_api(get_my_trades, symbol=convert_to_exchange_trading_pair(trading_pair))
        tasks = [get_my_trades(symbol=convert_to_exchange_trading_pair(trading_pair))
                         for trading_pair in trading_pairs]
        print("tasks:",tasks)
        print(f"Polling for order fills of {len(tasks)} trading pairs.")
        exchange_history = await safe_gather(*tasks, return_exceptions=True)
        for trades, trading_pair in zip(exchange_history, trading_pairs):
            if isinstance(trades, Exception):
                print(f"Error fetching trades update for the order {trading_pair}: {trades}.")
                #self.logger().network(f"Error fetching trades update for the order {trading_pair}: {trades}.", app_warning_msg=f"Failed to fetch trade update for {trading_pair}.")
                continue
            """
            for trade in trades:
                if self.is_confirmed_new_order_filled_event(str(trade["id"]), str(trade["orderId"]), trading_pair):
                    # Should check if this is a partial filling of a in_flight order.
                    # In that case, user_stream or _update_order_fills_from_trades will take care when fully filled.
                    if not any(trade["id"] in in_flight_order.trade_id_set for in_flight_order in self._in_flight_orders.values()):
                        c_trigger_event(self.MARKET_ORDER_FILLED_EVENT_TAG,
                                             OrderFilledEvent(
                                                 trade["time"],
                                                 self._exchange_order_ids.get(str(trade["orderId"]),
                                                                              get_client_order_id("buy" if trade["isBuyer"] else "sell", trading_pair)),
                                                 trading_pair,
                                                 TradeType.BUY if trade["isBuyer"] else TradeType.SELL,
                                                 OrderType.LIMIT_MAKER,  # defaulting to this value since trade info lacks field
                                                 Decimal(trade["price"]),
                                                 Decimal(trade["qty"]),
                                                 TradeFee(
                                                     percent=Decimal(0.0),
                                                     flat_fees=[(trade["commissionAsset"],
                                                                 Decimal(trade["commission"]))]
                                                 ),
                                                 exchange_trade_id=trade["id"]
                                             ))
                        print(f"Recreating missing trade in TradeFill: {trade}")
            """
def main():
    loop = asyncio.get_event_loop()
    #loop.run_until_complete(test_snapshot())
    #loop.run_until_complete(test_get_trading_pairs())
    #loop.run_until_complete(test_get_last_price())
    #loop.run_until_complete(test_get_fetch_trading_pairs())
    #loop.run_until_complete(test_ping_server())

    #global client
    # initialise the client
    #client = BinanceClient("","",None, "com", True)

    #loop.run_until_complete(_history_reconciliation())
    
    loop.close()

    print(convert_from_exchange_trading_pair("ethusd"))

main()
