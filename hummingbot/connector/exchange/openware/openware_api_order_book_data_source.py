#!/usr/bin/env python

import asyncio
import aiohttp
import logging
import pandas as pd
from typing import (
	Any,
	AsyncIterable,
	Dict,
	List,
	Optional
)
import time
import ujson
import websockets
from websockets.exceptions import ConnectionClosed

from hummingbot.client.config.global_config_map import global_config_map
from hummingbot.core.utils import async_ttl_cache
from hummingbot.core.utils.async_utils import safe_gather
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.order_book_tracker_entry import OrderBookTrackerEntry
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.logger import HummingbotLogger
from hummingbot.connector.exchange.openware.openware_order_book import OpenwareOrderBook

from hummingbot.connector.exchange.openware.openware_utils import (
	convert_to_exchange_trading_pair,
	convert_from_exchange_trading_pair
)

#openware_api_url = "https://www.quantaexchange.org/api/v2/peatio"
#openware_ranger_url = "wss://www.quantaexchange.org/api/v2/ranger"

class OpenwareAPIOrderBookDataSource(OrderBookTrackerDataSource):

	MESSAGE_TIMEOUT = 30.0
	PING_TIMEOUT = 10.0



	_baobds_logger: Optional[HummingbotLogger] = None

	@classmethod
	def logger(cls) -> HummingbotLogger:
		if cls._baobds_logger is None:
			cls._baobds_logger = logging.getLogger(__name__)
		return cls._baobds_logger

	def __init__(self, trading_pairs: List[str], openware_api_url, openware_ranger_url, domain="com"):
		super().__init__(trading_pairs)

		# HACK using global variables because sometimes NULL inputs come in later
		global _openware_ranger_url
		global _openware_api_url
		try:
			_openware_ranger_url
		except NameError:
			_openware_ranger_url = openware_ranger_url

		try:
			_openware_api_url
		except NameError:
			_openware_api_url = openware_api_url

		#self.openware_api_url = openware_api_url
		#self.openware_ranger_url = openware_ranger_url


		self._order_book_create_function = lambda: OrderBook()
		self._domain = domain

# API INTEGRATED
	@classmethod
	async def get_last_traded_price(cls, trading_pair: str, domain: str = "com") -> float:
		global _openware_api_url
		async with aiohttp.ClientSession() as client:
			url = _openware_api_url
			symbol = convert_to_exchange_trading_pair(trading_pair)
			url = f"{url}/public/markets/{symbol}/tickers"
			resp = await client.get(url)
			resp_json = await resp.json()
			return float(resp_json['ticker']['last'])
# API

# API INCLUDED FROM binance
	@classmethod
	async def get_last_traded_prices(cls, trading_pairs: List[str], domain: str = "com") -> Dict[str, float]:
		tasks = [cls.get_last_traded_price(t_pair, domain) for t_pair in trading_pairs]
		results = await safe_gather(*tasks)
		return {t_pair: result for t_pair, result in zip(trading_pairs, results)}
# API

# ERROR too different
# Could not implement get_all_mid_prices() for openware as there is no API to cover this case.
#	async def get_all_mid_prices(domain="com") -> Optional[Decimal]:
# ERROR too different

# API Untested
	@staticmethod
	async def fetch_trading_pairs(domain="com") -> List[str]:
		try:
			raw_trading_pairs = await get_trading_pairs()
			trading_pair_list: List[str] = []
			for raw_trading_pair in raw_trading_pairs:
				converted_trading_pair: Optional[str] = raw_trading_pair
				if converted_trading_pair is not None:
					trading_pair_list.append(converted_trading_pair)
			return trading_pair_list
		except Exception as e:
			#print(str(e))
			# Do nothing if the request fails -- there will be no autocomplete for openware trading pairs
			pass
		return []
# API Untested

	@classmethod
	@async_ttl_cache(ttl=60 * 30, maxsize=1)
	async def get_active_exchange_markets(cls) -> pd.DataFrame:
		"""
		Returned data frame should have trading_pair as index and include usd volume, baseAsset and quoteAsset
		"""
		global _openware_api_url
		async with aiohttp.ClientSession() as client:
			uri_market = "%s/public/markets" % _openware_api_url
			uri_ticker = "%s/public/markets/tickers" % _openware_api_url

			print(uri_market);
			print(uri_ticker);

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
			#print("trading_pairs!: ", trading_pairs) # PRINTY
			market_data: List[Dict[str, Any]] = [{**item, **exchange_data[item["id"]]["ticker"]}
													for item in market_data]

			# Build the data frame.
			all_markets: pd.DataFrame = pd.DataFrame.from_records(data=market_data, index="id")

			return all_markets.sort_values("last", ascending=False)

# API
	async def get_trading_pairs(self) -> List[str]:
		if not self._trading_pairs:
			try:
				active_markets: pd.DataFrame = await self.get_active_exchange_markets()
				self._trading_pairs = active_markets.index.tolist()
			except Exception:
				self._trading_pairs = []
				self.logger().network(
					f"Error getting active exchange information.",
					exc_info=True,
					app_warning_msg=f"Error getting active exchange information. Check network connection."
				)
		return self._trading_pairs
# API

# API
	@classmethod
	async def get_snapshot(cls, client: aiohttp.ClientSession, trading_pair: str, limit: int = 1000) -> Dict[str, Any]:
		global _openware_api_url
		params: Dict = {"limit": str(limit)} if limit != 0 else {}
		uri = "%s/public/markets/%s/depth" % (_openware_api_url, convert_to_exchange_trading_pair(trading_pair))
		#print("get_snapshot(): ", uri)
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
# API

# API INCLUDED FROM binance
	async def get_new_order_book(self, trading_pair: str) -> OrderBook:
		async with aiohttp.ClientSession() as client:
			snapshot: Dict[str, Any] = await self.get_snapshot(client, trading_pair, 1000)
			snapshot_timestamp: float = time.time()
			snapshot_msg: OrderBookMessage = OpenwareOrderBook.snapshot_message_from_exchange(
				snapshot,
				snapshot_timestamp,
				metadata={"trading_pair": trading_pair}
			)
			order_book = self.order_book_create_function()
			order_book.apply_snapshot(snapshot_msg.bids, snapshot_msg.asks, snapshot_msg.update_id)
			return order_book
# API

# API
	async def _inner_messages(self, ws: websockets.WebSocketClientProtocol) -> AsyncIterable[str]:
		try:
			while True:
				try:
					msg: str = await asyncio.wait_for(ws.recv(), timeout=self.MESSAGE_TIMEOUT)
					yield msg
				except asyncio.TimeoutError:
					try:
						pong_waiter = await ws.ping()
						await asyncio.wait_for(pong_waiter, timeout=self.PING_TIMEOUT)
					except asyncio.TimeoutError:
						raise
		except asyncio.TimeoutError:
			self.logger().warning("WebSocket ping timed out. Going to reconnect...")
			return
		except ConnectionClosed:
			return
		finally:
			await ws.close()
# API

# API

	@classmethod
	async def listen_for_trades(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
		global _openware_api_url
		params: Dict = {}
		trading_pair = "ETH-USD"
		uri = "%s/public/markets/%s/order-book" % (_openware_api_url, convert_to_exchange_trading_pair(trading_pair))
		#print("listen_for_trades() NEW: ", uri) # PRINTY
		while True:
			try:
				async with aiohttp.ClientSession() as client:
					async with client.get(uri, params=params) as response:
						response: aiohttp.ClientResponse = response
						if response.status != 200:
							raise IOError(f"Error fetching Openware market orderbook for {trading_pair}. "
										  f"HTTP status is {response.status}.")
						data: Dict[str, Any] = await response.json()
				
						for key in data:
							if key == "asks":
								#print("ask: ", len(data[key]))
								for order in data[key]:
									#print("ask:", order)

									trade_msg: OrderBookMessage = OpenwareOrderBook.trade_message_from_exchange({
										'trade': order,
										'trading_pair': order['market']
									})
									output.put_nowait(trade_msg)
							if key == "bids":
								#print("bids: ", len(data[key]))
								for order in data[key]:
									#print("bid:", order)

									trade_msg: OrderBookMessage = OpenwareOrderBook.trade_message_from_exchange({
										'trade': order,
										'trading_pair': order['market']
									})
									output.put_nowait(trade_msg)
				await asyncio.sleep(4.0)
			except asyncio.CancelledError:
				raise
			except Exception:
				self.logger().error("Error fetching Retrying after 30 seconds...", exc_info=True)
				await asyncio.sleep(30.0)

	async def listen_for_trades_OLD(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
		#print("listen_for_trades(): ")
		global _openware_ranger_url
		while True:
			try:
				trading_pairs: List[str] = await self.get_trading_pairs()
				ws_path: str = "&stream=".join([f"{trading_pair.lower()}.trades" for trading_pair in trading_pairs])
				stream_url: str = f"{_openware_ranger_url}/public/?stream={ws_path}"
				print("listen_for_trades(): ", stream_url)
				async with websockets.connect(stream_url) as ws:
					ws: websockets.WebSocketClientProtocol = ws
					async for raw_msg in self._inner_messages(ws):
						msg = ujson.loads(raw_msg)
						print("[TRADE1]", msg)
						for key in msg:
							t_pairs = key.split('.')[0]
							for key1 in msg[key]:
								if 'trades' in msg[key][key1]:
									trades = msg[key][key1]
									for item in trades:
										print("[TRADE2]", item, t_pairs) # PRINTY
										trade_msg: OrderBookMessage = OpenwareOrderBook.trade_message_from_exchange({
											trade: item,
											trading_pair: t_pairs
										})
										output.put_nowait(trade_msg)
			except asyncio.CancelledError:
				raise
			except Exception:
				self.logger().error("Unexpected error with WebSocket connection. Retrying after 30 seconds...", exc_info=True)
				await asyncio.sleep(30.0)
# API

# API
	async def listen_for_order_book_diffs(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
		#print("listen_for_order_book_diffs(): ")
		global _openware_ranger_url
		while True:
			try:
				trading_pairs: List[str] = await self.get_trading_pairs()
				ws_path: str = "&stream=".join([f"{trading_pair.lower()}.update" for trading_pair in trading_pairs])
				stream_url: str = f"{_openware_ranger_url}/public/?stream={ws_path}"

				async with websockets.connect(stream_url) as ws:
					ws: websockets.WebSocketClientProtocol = ws
					async for raw_msg in self._inner_messages(ws):
						msg = ujson.loads(raw_msg)
						if "success" in msg:
							pass
						#else:
						#	 order_book_message: OrderBookMessage = OpenwareOrderBook.diff_message_from_exchange(
						#		 msg, time.time())
						#	 output.put_nowait(order_book_message)
			except asyncio.CancelledError:
				raise
			except Exception:
				self.logger().error("Unexpected error with WebSocket connection. Retrying after 30 seconds...",
									exc_info=True)
				await asyncio.sleep(30.0)
# API

# API
	async def listen_for_order_book_snapshots(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
		#print("listen_for_order_book_snapshots(): ")
		while True:
			try:
				trading_pairs: List[str] = await self.get_trading_pairs()
				async with aiohttp.ClientSession() as client:
					for trading_pair in trading_pairs:
						try:
							snapshot: Dict[str, Any] = await self.get_snapshot(client, trading_pair)
							snapshot_timestamp: float = time.time()
							snapshot_msg: OrderBookMessage = OpenwareOrderBook.snapshot_message_from_exchange(
								snapshot,
								snapshot_timestamp,
								{"trading_pair": trading_pair}
							)
							output.put_nowait(snapshot_msg)
							self.logger().debug(f"Saved order book snapshot for {trading_pair}")
							print(f"Saved order book snapshot for {trading_pair}")
							await asyncio.sleep(5.0)
						except asyncio.CancelledError:
							raise
						except Exception:
							self.logger().error("Unexpected error.", exc_info=True)
							await asyncio.sleep(5.0)
					this_hour: pd.Timestamp = pd.Timestamp.utcnow().replace(minute=0, second=0, microsecond=0)
					next_hour: pd.Timestamp = this_hour + pd.Timedelta(hours=1)
					delta: float = next_hour.timestamp() - time.time()
					await asyncio.sleep(delta)
			except asyncio.CancelledError:
				raise
			except Exception:
				self.logger().error("Unexpected error.", exc_info=True)
				await asyncio.sleep(5.0)
# API