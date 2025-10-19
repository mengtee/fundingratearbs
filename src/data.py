import os
import asyncio
import logging
import json
import numpy as np

from pprint import pprint
from dotenv import load_dotenv
import collections
load_dotenv()

from quantpylib.gateway.master import Gateway

logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')

def get_key():
    config_keys ={
        "hyperliquid":{
            "key": os.getenv("HYPERLIQUID_API_KEY"),
            "secret": os.getenv("HYPERLIQUID_API_SECRET")
        },
        "paradex":{
            "key": os.getenv("PARADEX_API_KEY"),
            "secret": os.getenv("PARADEX_API_SECRET") ,
            "l2_secret": os.getenv("PARADEX_L2_SECRET")
        },
    }
    
    return config_keys
    
class MarketData():
    def __init__(self, gateway, exchanges, preference_quote=["USDT","USD"]):
        self.gateway = gateway
        self.exchanges = exchanges
        self.universe = {}
        self.preference_quote = preference_quote
        
        self.base_mappings = {exc:{} for exc in exchanges} #binance: BTC:BTCUSDT, hyperliquid: BTC:BTC
        self.l2_ticker_streams = {exc:{} for exc in exchanges}
        self.balances = {exc:None for exc in exchanges}
    
    def get_balance(self, exchange):
        return self.balances[exchange]
    
    def get_base_mappings(self, exchange):
        return self.base_mappings[exchange]
    
    def get_l2_stream(self, exchange, ticker):
        return self.l2_ticker_streams[exchange][ticker]
    
    # primary entry 
    async def serve_exchanges(self):
        return await asyncio.gather(*[
            self.serve(exc) for exc in self.exchanges
        ])
        
    async def serve(self, exchange):
        return await asyncio.gather(*[
            self.serve_base_mappings(self.gateway, exchange),
            self.serve_l2_stream(self.gateway, exchange),
            self.serve_balance(self.gateway, exchange)
        ])
    
    async def serve_base_mappings(self, gateway, exchange):
        while True:
            try:
                perps_data = await gateway.exchange.contract_specifications(exc=exchange)
                funding_data = await gateway.exchange.get_funding_info(exc=exchange)
                
                # pprint(f"Fetched base mappings for {exchange}")
                # pprint(f"perps_data sample: {list(perps_data.items())[:2]}")
                # print(f"funding_data sample: {list(funding_data.items())[:2]}")
                mappings = collections.defaultdict(list)
                with open('src/assets.txt','r') as f:
                    assets = [line.strip() for line in f.readlines() if line != '']
                
                if exchange == 'binance':
                    with open('binance_fr.json','r') as f:
                        fr_data = json.load(f)
                        
                elif exchange == "paradex":
                    with open('paradex_fr.json','r') as f:
                        fr_data = json.load(f)
                        
                for k, v in perps_data.items(): # filtering base assets
                    if v['base_asset'] in assets and v["quote_asset"] in ["USDT","USDC", "USD"]:
                        if 'PERP' in k or not any(x in k for x in ['-C', '-P']): # filtering out options contracts
                            funding_info = funding_data.get(k, {})
                            combined_data = {**v, **funding_info, 'symbol': k}
                            mappings[v['base_asset']].append(combined_data)
                        
                remove = set() 
                for k, v in mappings.items(): # filtering quote assets
                    if len(v) >1 and self.preference_quote is not None:
                        mappings[k] = [x for x in v if x['quote_asset'] in self.preference_quote]
                    if len(mappings[k]) == 0:
                        remove.add(k)
                for k in remove:
                    mappings.pop(k)
                
                if exchange == 'binance':
                    for k, v in mappings.items():
                        for contract in v:
                                assert contract['symbol'] in fr_data
                                contract['frint'] = np.float64(fr_data[contract['symbol']])
                            
                elif exchange == 'paradex':
                    for k, v in mappings.items():
                        for contract in v:
                            assert contract['symbol'] in fr_data
                            contract['frint'] = np.float64(fr_data[contract['symbol']])
                            
                mappings = dict(sorted(mappings.items()))
                self.base_mappings[exchange] = mappings
                self.universe[exchange] = mappings
                
                await asyncio.sleep(60*4)
                
            except Exception as e:
                logging.error(f"[{exchange}] Error in fetching base mappings: {e}")
                await asyncio.sleep(15)
    
    async def serve_l2_stream(self, gateway, exchange):
        universe = self.universe
        while not universe or exchange not in universe:
            await asyncio.sleep(5)
            
        with open('src/assets.txt', 'r') as f:
            assets = [line.strip() for line in f.readlines() if line != ""]
        base_mappings = universe[exchange]
        
        tickers=[]
        for asset in assets:
            if asset in base_mappings:
                tickers.extend([ticker['symbol']for ticker in base_mappings[asset]])
        
        logging.info(f"[{exchange}] L2 Ticker Streams: {tickers}")
        
        if exchange == 'binance':tickers.append('USDCUSDT') # for different asset price quote
        
        lobs = await asyncio.gather(*[
            gateway.executor.l2_book_mirror(
                ticker=ticker,
                speed_ms=500,
                exc=exchange,
                depth=20,
                as_dict=False,
                buffer_size=10,
            )for ticker in tickers
        ])
        
        for ticker, lob in zip(tickers, lobs):
            self.l2_ticker_streams[exchange][ticker] = lob
        logging.info(f"Updated L2 streams for {exchange} and tickers: {list(self.l2_ticker_streams[exchange].keys())}")
        
        await asyncio.sleep(1e9)
    
    async def serve_balance(self, gateway, exchange, poll_interval=100):
        while True:
            try:
                bal = await gateway.account.account_balance(exc=exchange)
                self.balances[exchange] = bal
                await asyncio.sleep(poll_interval)
                
            except Exception as e:
                logging.error(f"[{exchange}] Error in fetching balances: {e}")
                await asyncio.sleep(15)

async def main():
    gateway = Gateway(config_keys=get_key())
    
    await gateway.init_clients()
    
    market_data = MarketData(
        gateway=gateway,
        # exchanges=["binance","hyperliquid","lighter","paradex"]
        exchanges=["hyperliquid", "paradex"],
        preference_quote="USDT"
    )
    
    asyncio.create_task(market_data.serve_exchanges())
    
    await asyncio.sleep(30*90)
    
    await gateway.cleanup_clients()
    
if __name__ == "__main__":
    asyncio.run(main())
    