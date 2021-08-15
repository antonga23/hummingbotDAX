import hashlib
import hmac
import requests
import time
import urllib

import asyncio
import aiohttp
from typing import (
    Any,
    AsyncIterable,
    Dict,
    List,
    Optional,
    Tuple
)

from typing import Dict, Optional, List, Tuple

from decimal import Decimal
from binance.client import Client as BinanceClient

openware_api_url = 'https://www.quantaexchange.org/api/v2/peatio'

class OpenwareClient():
    def __init__(self, openware_api_key: str, openware_api_secret: str, tld="com"):
        self.openware_api_key = openware_api_key
        self.openware_api_secret = openware_api_secret

    async def ping_server(self) -> Dict[str, Any]:
        params: Dict = {"limit": 1}
        uri = f"{openware_api_url}/public/health/alive"
        async with aiohttp.ClientSession() as client:
            async with client.get(uri, params=params) as response:
                response: aiohttp.ClientResponse = response
                if response.status != 200:
                    raise IOError("Error pinging Openware server might be down. Check connection or API KEYS and try again.")
                data = await response.text()
                if data == "200":
                    return {}
                else:
                    raise IOError("Error pinging Openware server might be down. Check connection or API KEYS and try again.")

    async def ping(self):
        result = await self.ping_server()
        return result

    def get_account_test(self):
        result = {
            'makerCommission': 10,
            'takerCommission': 10,
            'buyerCommission': 0,
            'sellerCommission': 0,
            'canTrade': True,
            'canWithdraw': True,
            'canDeposit': True,
            'updateTime': 1625434293087,
            'accountType': 'SPOT',
            'balances': [{
                "asset": "ETHUSD",
                "free": 9.9,
                "locked": 0.1
            }],
            'permissions': ['SPOT']
        }
        balances = result["balances"]
        for balance_entry in balances:
            asset_name = balance_entry["asset"]
            free_balance = Decimal(balance_entry["free"])
            total_balance = Decimal(balance_entry["free"]) + Decimal(balance_entry["locked"])
            print(asset_name)
            print(total_balance)
            print(free_balance)
        return result;
    
    async def get_account(self):
        result = await get_account_test()
        return result
    
    def get_exchange_info(self):
        print("get_exchange_info()")

    def get_my_trades(self):
        print("get_my_trades()")

    def get_order(self):
        print("get_order()")

    def get_account(self):
        print("get_account()")

    def create_order(self):
        print("create_order()")

    def cancel_order(self):
        print("create_order()")

    def get_open_orders(self):
        print("create_order()")

clientOpenware = OpenwareClient("9d833da4f5651cc4","f9b5a592b79cc73e712d99179b246128","com")

async def test_ping_server():
    print("Test openware ping server")
    result = await clientOpenware.ping()
    print(result)

async def test_get_account():
    print("Test openware get account")
    result = await clientOpenware.get_account()
    print(result)
    return 0
        
def main():
    print("TESTING Openware client")
    loop = asyncio.get_event_loop()
    #loop.run_until_complete(test_ping_server())
    #loop.run_until_complete(test_get_account())
    clientOpenware.get_account_test()
    
    global client
    # initialise the client
    client = BinanceClient("RhTLkhQeROvQNh7J2rzkHm9v0bDzV0DZADK190D7wrC1nyf4xixgR37fGAYzb42E","mLxuC60xsmATQEDLcJKOsqugmePSFYoYzVtBngAtQbMIxDPHEdbo0tMUiNicRxQm",None, "com", False)

    loop.close()
main()
