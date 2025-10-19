import asyncio
import msgpack
import numpy as np
import pandas as pd
from decimal import Decimal
import time
import json
import websockets
from collections import defaultdict

from eth_utils import keccak, to_hex
from eth_account import Account
from eth_account.messages import encode_typed_data

from quantpylib.utilities.math import str_normalized_float, nsigfigs
from numpy_ringbuffer import RingBuffer
from quantpylib.throttler.aiohttp import HTTPClient

endpoints = {
    "order_market": {
        
    },
    "orders_create": {},
    "orders_cancel": {},
    "orders_cancel_cloids": {},
    
    "all_mids": {
        "endpoint": "/info",
        "method": "POST",
        "data": {"type": "allMids"}
    },
    "orders_open": {
        "endpoint": "/info",
        "method": "POST",
        "data": {
            "type": "openOrders"
        }
    },
    "perpetuals_metadata": {
        "endpoint": "/info",
        "method": "POST",
        "data": {
            "type":"meta",
        }
    },
    "perpetuals_contexts": {
        "endpoint": "/info",
        "method": "POST",
        "data": {
            "type": "metaAndAssetCtxs"
        }
    },
    "perpetuals_account": {
        "endpoint": "/info",
        "method": "POST",
        "data":{
            "type": "clearinghouseState"
        }
    },
}


MAINNET_URL = "https://fapi.asterdex.com"

def address_to_bytes(address):
    return bytes.fromhex(address[2:] if address.startswith("0x") else address)

def action_hash(action, vault_address, nonce, expires_after):
    data = msgpack.packb(action)
    data += nonce.to_bytes(8, "big")
    if vault_address is None:
        data += b"\x00"
    else:
        data += b"\x01"
        data += address_to_bytes(vault_address)
    if expires_after is not None:
        data += b"\x00"
        data += expires_after.to_bytes(8, "big")
    return keccak(data)

def sign_inner(wallet, data):
    structured_data = encode_typed_data(full_message=data)
    signed = wallet.sign_message(structured_data)
    return {"r": to_hex(signed["r"]), "s": to_hex(signed["s"]), "v": signed["v"]}

def user_signed_payload(primary_type, payload_types, action):
    chain_id = int(action["signatureChainId"], 16)
    return {
        "domain": {
            "name": "HyperliquidSignTransaction",
            "version": "1",
            "chainId": chain_id,
            "verifyingContract": "0x0000000000000000000000000000000000000000",
        },
        "types": {
            primary_type: payload_types,
            "EIP712Domain": [
                {"name": "name", "type": "string"},
                {"name": "version", "type": "string"},
                {"name": "chainId", "type": "uint256"},
                {"name": "verifyingContract", "type": "address"},
            ],
        },
        "primaryType": primary_type,
        "message": action,
    }
    
def sign_user_signed_action(wallet, action, payload_types, primary_type, is_mainnet):
    # signatureChainId is the chain used by the wallet to sign and can be any chain.
    # hyperliquidChain determines the environment and prevents replaying an action on a different chain.
    action["signatureChainId"] = "0x66eee"
    action["hyperliquidChain"] = "Mainnet" if is_mainnet else "Testnet"
    data = user_signed_payload(primary_type, payload_types, action)
    return sign_inner(wallet, data)

def sign_l1_action(wallet, action, active_pool, nonce, expires_after, is_mainnet):
    hash = action_hash(action, active_pool, nonce, expires_after) # convert the address to bytes
    phantom_agent = construct_phantom_agent(hash, is_mainnet)
    data = l1_payload(phantom_agent)
    return sign_inner(wallet, data)

def construct_phantom_agent(hash, is_mainnet):
    return {"source": "a" if is_mainnet else "b", "connectionId": hash}

def l1_payload(phantom_agent): # Preparing EIP-712 payload
    return {
        "domain": {
            "chainId": 1337,
            "name": "Exchange",
            "verifyingContract": "0x0000000000000000000000000000000000000000",
            "version": "1",
        },
        "types": {
            "Agent": [
                {"name": "source", "type": "string"},
                {"name": "connectionId", "type": "bytes32"},
            ],
            "EIP712Domain": [
                {"name": "name", "type": "string"},
                {"name": "version", "type": "string"},
                {"name": "chainId", "type": "uint256"},
                {"name": "verifyingContract", "type": "address"},
            ],
        },
        "primaryType": "Agent",
        "message": phantom_agent,
    }

class Hyperliquid():
    def __init__(self, key="", secret= "",**kwargs):
        self.key = key
        self.secret = secret
        self.wallet = Account.from_key(secret) if secret else None
        self.aws_manager = AsyncWebSocketManager(key=key)
        
        if self.wallet:
            self.vault_address = None
            self.account_address = self.wallet.address
            # self.vault_address = self.key if self.wallet.address.lower()!= self.key.lower() else None
            # self.account_address = self.wallet.address
            
        else:
            self.vault_address = None
            self.account_address = None
        
        self.http_client = HTTPClient(base_url= MAINNET_URL, client_args={"verify_ssl":False})
        self.nonce_buffer = RingBuffer(capacity=50, dtype=np.uint64) #circular buffer storage for nonce
        self.nonce_buffer._arr[:] = 0
        
    
    async def init_client(self):
        perp_meta = await self.perpetuals_metadata()
        perp_mapping = {v["name"]: k for k, v in enumerate(perp_meta["universe"])}
        
        self.ticker_to_idx = perp_mapping
        self.ticker_to_lot_precision = {v['name']:int(v['szDecimals'])for v in perp_meta['universe']}
        self.ticker_to_price_precision = {v['name']: 6 -self.ticker_to_lot_precision[v['name']] for v in perp_meta['universe']}
        return
    
    def _add_to_request(self, endpoint, headers=None, params= None, data=None):
        endpoint = endpoints[endpoint]
        if headers:
            if 'headers' in endpoint:
                endpoint['headers'].update(headers)
            else:
                endpoint["headers"] = headers
        
        if params:
            if 'params' in endpoint:
                endpoint['params'].update(params)
            else:
                endpoint["params"] = params 
                
        if data:
            if 'data' in endpoint:
                endpoint['data'].update(data)
            else:
                endpoint["data"] = data
                
        return endpoint
    
    def _add_address(self, endpoint):
        # IMPORTANT: Account information need to use back the public key (API wallet address and private address derrived are for signing only)
        return self._add_to_request(endpoint, data={"user": self.key}) 
    
    async def position_get(self):
        pass
    
    async def _slippage_price(self, ticker, is_long, slippage):
        mids = await self.all_mids()
        mid = float(mids[ticker])
        
        price = mid *(1+slippage) if is_long else mid *(1-slippage)
        return nsigfigs(round(price, self.ticker_to_price_precision[ticker]),5)
                
    async def order_market(self, ticker, amount, reduce_only=False, cloid=None, slippage_tolerance=0.05):
        aggressive_price = await self._slippage_price(ticker, is_long = amount>0, slippage = slippage_tolerance)
        
        return await self.order_create(
            ticker=ticker,
            amount=amount,
            price=aggressive_price,
            reduce_only=reduce_only,
            cloid=cloid,
        )
            
    async def request_l1_action(self, action, nonce, signature, expires_after=None):
        
        return await self.http_client.request(
            endpoint="/exchange",
            method="POST",
            data={
                "action": action,
                "nonce":nonce,
                "signature": signature,
                "vaultAddress": self.vault_address,
                **({"expiresAfter": expires_after} if expires_after is not None else {})
            }
        )       
    
    async def order_create(self, ticker, amount, price, tif="Gtc", reduce_only=False, cloid=None, grouping="na"):
        sz = abs(float(amount))
        ord_type = {"limit": {"tif": tif}}
        _price = nsigfigs(round(price, self.ticker_to_price_precision[ticker]), 5)
        _sz = round(sz, self.ticker_to_lot_precision[ticker])
        action = {
            "a":self.ticker_to_idx[ticker],
            "b": True if amount > 0 else False,
            'p': str_normalized_float(_price),
            's': str_normalized_float(_sz),
            'r': reduce_only,
            't': ord_type,
            **({"c":cloid} if cloid is not None else {}),
        }
        return await self.orders_create(
            order_actions=[action],
            grouping=grouping
        )
    
    async def orders_create(self, order_actions, grouping):
        nonce = self.get_nonce()
        order_action = {
            "type": "order",
            "orders": order_actions,
            "grouping": grouping
        } 
        signature = sign_l1_action(
            wallet= self.wallet,
            action=order_action, 
            active_pool=self.vault_address,
            nonce=nonce,
            expires_after= None,
            is_mainnet=True
        )
        
        return await self.request_l1_action(
            action=order_action,
            nonce=nonce,
            signature=signature   
        )
        
    def get_nonce(self):
        nonce =  int(time.time() * 1000)
        
        if nonce in self.nonce_buffer._arr:
            nonce = np.max(self.nonce_buffer._arr) + 1
        self.nonce_buffer.append(nonce)
        
        return nonce
    
    async def order_cancel(self, ticker, oid):
        action={
            "a": self.ticker_to_idx[ticker],
            "o": int(oid)
        }
        return await self.orders_cancel(
            order_actions=[action],
        )
    
    async def orders_cancel(self, order_actions):
        nonce = self.get_nonce()
        cancel_action = {
            "type": "cancel",
            "cancels": order_actions,
        } 
        signature = sign_l1_action(
            wallet= self.wallet,
            action=cancel_action, 
            active_pool=self.vault_address,
            nonce=nonce,
            expires_after= None,
            is_mainnet=True
        )
        
        return await self.request_l1_action(
            action=cancel_action,
            nonce=nonce,
            signature=signature   
        )
    
    async def orders_cancel_cloids(self):
        pass
    
    async def all_mids(self):
        return await self.http_client.request(**endpoints["all_mids"])
    
    async def orders_open(self):
        return await self.http_client.request(**self._add_address("orders_open"))    
    
    async def perpetuals_metadata(self):
        return await self.http_client.request(**endpoints["perpetuals_metadata"])
    
    async def perpetuals_contexts(self):
        return await self.http_client.request(**endpoints["perpetuals_contexts"])
    
    async def perpetuals_account(self):
        return await self.http_client.request(**self._add_address("perpetuals_account"))


class Channel():
    ALL_MIDS = "allMids"
    L2BOOK ="l2Book"
    TRADES = "trades"
    ORDER_UPDATES = "orderUpdates"
    USER_FILLS = "userFills"

def subscription_to_identifier(subscription):
    return str(subscription)

def identifier_to_subscription(id):
    import ast
    return ast.literal_eval(id) # Get back the dictionary for string encoded dict

class AsyncWebSocketManager():
    def __init__(self, key=""):
        self.ws_url = "wss://api.hyperliquid.xyz/ws"
        self.key = key
        self.socket_lock = asyncio.Lock()
        self.channels = defaultdict(lambda: None) #channels: socket
        self.subscriptions = defaultdict(set) #channel: st(subid)
        
        self.multiplex_channels = [Channel.L2BOOK, Channel.TRADES] #channels: subscriptions that are one-many 
        self.callbacks = {}
        
        for channel in self.multiplex_channels:
            self.callbacks[channel] = {}
            
        self.listeners = defaultdict(lambda:None) # listening tasks per channel
    
    
    async def ping(self, conn):
        try:
            while True:
                await conn.send(json.dump({"method": "ping"}))
                await asyncio.sleep(50)
        except:
            pass
            
    async def pong(self, conn):
        while True:
            pass
    
    async def listen_channel(self, channel):
        while True:
            socket = self.channels[channel]
            response = await socket.recv()
            print(json.loads(response))
      
    async def _send(self, channel, msg):
        msg = msg if isinstance(msg, str) else json.dumps(msg)
        await self.connect_channel(channel)
        await self.channels[channel].send(msg)
    
    async def connect_channel(self, channel):
        await self.socket_lock.acquire()
        if channel in self.channels:
            self.socket_lock.release()
            return
        else:
            conn = await websockets.connect(self.ws_url)
            self.channels[channel] = conn
            
            if not self.listeners[channel]:
                self.listeners[channel] = asyncio.create_task(self.listen_channel(channel))
            
            self.socket_lock.release()
            await asyncio.sleep(0)
        
    async def subscribe(self, channel, subscription, handler, callback_target=None):
        subid = subscription_to_identifier(subscription)
        if subid in self.subscriptions[channel]:
            return
        else:
            self.subscriptions[channel].add(subid)
            msg = {"method": "subscribe", "subscription": subscription}
            await self._send(channel,msg)
    
    async def l2_book_subscribe(self, ticker, handler):
        subscription = {"type": Channel.L2BOOK, "coin": ticker}
        return await self.subscribe(
            Channel.L2BOOK, 
            subscription, 
            handler, 
            callback_target=ticker
        )
        
    async def trades_subscribe(self, ticker, handler):
        subscription = {"type": Channel.TRADES, "coin": ticker}
        return await self.subscribe(
            Channel.TRADES, 
            subscription, 
            handler, 
            callback_target=ticker
        )
        
    async def all_mids_subscribe(self, handler):
        subscription = {"type": Channel.ALL_MIDS}
        return await self.subscribe(
            Channel.L2BOOK, 
            subscription, 
            handler, 
        )
             