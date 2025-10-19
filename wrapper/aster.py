import asyncio
import numpy as np
from decimal import Decimal
import time
import json
import websockets
from collections import defaultdict

import hmac
import hashlib

import copy
from urllib.parse import urlencode
from .enums.aster import *
from quantpylib.utilities.math import str_normalized_float, nsigfigs
from numpy_ringbuffer import RingBuffer
from quantpylib.throttler.aiosonic import HTTPClient

endpoints = {
    "order_create": {
        "endpoint": "/fapi/v1/order",
        "method": "POST",
    },
    "order_cancel": {
        "endpoint": "/fapi/v1/order",
        "method": "DELETE",
    },
    "orders_cancel": {
        "endpoint": "/fapi/v1/allOpenOrders",
        "method": "DELETE",
    },
    "orders_cancel_cloids": {},
    
    "order_leverage":{
        "endpoint": "/fapi/v1/leverage",
        "method": "POST",
    },
    "all_mids": {
        "endpoint": "/fapi/v1/premiumIndex",
        "method": "GET",
    },
    "orders_open": {
        "endpoint":"/fapi/v1/openOrders",
        "method": "GET",
    },
    "perpetuals_metadata": {
        "endpoint": "/fapi/v1/exchangeInfo",
        "method": "GET",
    },
    "perpetuals_contexts": {},
    "perpetuals_account": {},
}


MAINNET_URL = "https://fapi.asterdex.com"

def create_hmac_signature(secret, message):
    signature = hmac.new(
        secret.encode('utf-8'),
        message.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()
    
    return signature

class Aster():
    def __init__(self, key="", secret= "",**kwargs):
        self.key = key
        self.secret = secret
        
        self.http_client = HTTPClient(base_url= MAINNET_URL, client_args={"verify_ssl":False})
        self.nonce_buffer = RingBuffer(capacity=50, dtype=np.uint64) #circular buffer storage for nonce
        self.nonce_buffer._arr[:] = 0
    
    async def init_client(self):
        specs = await self.contract_specification()
        self.symbol_specs = specs
        
        self.ticker_to_price_precision = {
            ticker: spec["pricePrecision"] for ticker, spec in specs.items()
        }
        self.ticker_to_lot_precision = {
            ticker: spec["quantityPrecision"] for ticker, spec in specs.items()
        }
        return
    
    def _add_to_request(self, endpoint, headers=None, params= None, data=None):
        template = copy.deepcopy(endpoints[endpoint])
        
        if headers:
            template.setdefault("headers", {}).update(headers)
        if params:
            template.setdefault("params", {}).update(params)
        if data:
            template.setdefault("data", {}).update(data)
        
        return template
    
    def _add_address(self, endpoint):
        # IMPORTANT: Account information need to use back the public key (API wallet address and private address derrived are for signing only)
        return self._add_to_request(endpoint, data={"user": self.key}) 
    
    async def position_get(self):
        pass
    
    def _snap(self, value: Decimal, step: Decimal):
        if step ==0:
            return value
        return (value // step) *step
    
    async def _get_mid_price(self, ticker):
        mids = await self.all_mids()
        
        if isinstance(mids, list):
            for entry in mids:
                if entry["symbol"] == ticker:
                    return Decimal(entry["markPrice"])
        elif isinstance(mids, dict):
            # some integrations return a mapping instead of a list
            data = mids.get(ticker) or {}
            value = data.get("markPrice") or data.get("mid") or data
            if value is not None:
                return Decimal(str(value))
        raise ValueError(f"{ticker}: mark price not found in premiumIndex response")
        
    
    async def _apply_filter(self, symbol: str, price: Decimal, qty: Decimal, is_market: bool):
        spec = self.symbol_specs[symbol]

        # quantity always needs to be on an allowed step
        qty_step = spec["marketStepSize"] if is_market and spec["marketStepSize"] else spec["stepSize"]
        qty_adj = self._snap(qty, qty_step)

        if not is_market:
            price_adj = self._snap(price, spec["tickSize"])
            if price_adj < spec["minPrice"] or (spec["maxPrice"] != 0 and price_adj > spec["maxPrice"]):
                raise ValueError(f"{symbol}: price {price_adj} outside [{spec['minPrice']}, {spec['maxPrice']}]")
        else:
            # mark price → estimate notional, but keep price_adj at zero so we don't send it
            mark = await self._get_mid_price(symbol)
            price_adj = mark

        if qty_adj < spec["minQty"] or qty_adj > spec["maxQty"]:
            raise ValueError(f"{symbol}: quantity {qty_adj} outside [{spec['minQty']}, {spec['maxQty']}]")

        if spec["minNotional"]:
            est_notional = price_adj * qty_adj
            if est_notional < spec["minNotional"]:
                raise ValueError(f"{symbol}: notional {est_notional} below minimum {spec['minNotional']}")

        return price_adj if not is_market else Decimal("0"), qty_adj

    async def contract_specification(self):
        exchange_info = await self.perpetuals_metadata()
        
        specs = {}
        for symbol in exchange_info['symbols']:
            if symbol['status'] != "TRADING":
                continue
            
            filters = {f["filterType"]: f for f in symbol["filters"]}
            price_filter = filters['PRICE_FILTER']
            lot_filter = filters['LOT_SIZE']
            market_lot_filter = filters.get('MARKET_LOT_SIZE')
            min_notional = filters.get('MIN_NOTIONAL')
            
            specs[symbol["symbol"]] = {
                "pricePrecision": symbol["pricePrecision"],
                "quantityPrecision": symbol["quantityPrecision"],
                "tickSize": Decimal(price_filter["tickSize"]),
                "minPrice": Decimal(price_filter["minPrice"]),
                "maxPrice": Decimal(price_filter["maxPrice"]),
                "stepSize": Decimal(lot_filter["stepSize"]),
                "minQty": Decimal(lot_filter["minQty"]),
                "maxQty": Decimal(lot_filter["maxQty"]),
                "marketStepSize": Decimal(market_lot_filter["stepSize"]) if market_lot_filter else None,
                "marketMinQty": Decimal(market_lot_filter["minQty"]) if market_lot_filter else None,
                "marketMaxQty": Decimal(market_lot_filter["maxQty"]) if market_lot_filter else None,
                "minNotional": Decimal(min_notional["notional"]) if min_notional else None,
            }
        
        self.symbol_specs = specs
        
        return specs
        
    async def _authenticated_request(self, endpoint_key, params=None, headers=None):
        request_params = dict(params or {})
        request_params["timestamp"] = int(time.time() * 1000)
        request_params["recvWindow"] = 5000
        
        query_string = urlencode(request_params)
        signature = create_hmac_signature(self.secret, query_string)
        
        request_params["signature"] = signature        
        auth_headers = {"X-MBX-APIKEY": self.key, "Content-Type": "application/x-www-form-urlencoded"}
        
        if headers:
            auth_headers.update(headers)
            
        endpoint_config = self._add_to_request(endpoint_key, headers=auth_headers, params=request_params)
        
        return await self.http_client.request(**endpoint_config)
        
    async def order_create(self, ticker, side, position_size, price=None, type = OrderType.LIMIT, tif=TimeInForce.GTC, reduce_only=False):        
        if type != OrderType.MARKET and price is None:
            raise ValueError("Price required for non-market orders")
        
        price_dec = Decimal(str(price)) if price is not None else Decimal("0")
        size_dec = Decimal(str(position_size))

        adj_price, adj_size = await self._apply_filter(
            ticker,
            price_dec,
            size_dec,
            is_market=(type == OrderType.MARKET),
        )

        params = {
            "symbol": ticker,
            "side": side.value,
            "type": type.value,
            "quantity": str_normalized_float(adj_size),
        }
        
        if type == OrderType.LIMIT:
            params["price"] = str_normalized_float(adj_price)
            params["timeInForce"] = tif.value
        if reduce_only:
            params["reduceOnly"] = "true"

        return await self._authenticated_request("order_create", params=params)

    async def order_cancel(self, ticker, order_id, orig_client_order_id=None, recv_window=5000):
        params={
            "symbol": ticker,
        }
        
        if order_id:
            params["orderId"] = order_id
        elif orig_client_order_id:
            params["origClientOrderId"] = orig_client_order_id
        
        return await self._authenticated_request("order_cancel", params=params)
       
    async def orders_cancel(self, ticker):
       params = {"symbol": ticker}
       return await self._authenticated_request("orders_cancel", params=params)
    
    async def orders_cancel_cloids(self):
        pass
    
    async def order_leverage(self, ticker, leverage):
        return await self._authenticated_request("order_leverage", params={
            "symbol": ticker,
            "leverage": leverage
        })
    
    async def orders_open(self):
        return await self._authenticated_request("orders_open")
    
    async def all_mids(self):
        return await self.http_client.request(**endpoints["all_mids"])

    async def perpetuals_metadata(self):
        return await self.http_client.request(**endpoints["perpetuals_metadata"])
    
    async def perpetuals_contexts(self):
        return await self.http_client.request(**endpoints["perpetuals_contexts"])
    
    async def perpetuals_account(self):
        return await self.http_client.request(**self._add_address("perpetuals_account"))
