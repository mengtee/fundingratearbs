import numpy as np
import asyncio
from data import MarketData, get_key
import pytz
from datetime import datetime, timedelta
import logging
import time
import math
from decimal import Decimal
from quant_telegram import TelegramBot
from pprint import pprint
from quantpylib.gateway.master import Gateway
import quantpylib.standards.markets as mk
import collections
'''
def get_balance(self, exchange):
    return self.balances[exchange]
    
def get_base_mappings(self, exchange):
    return self.base_mappings[exchange]

def get_l2_stream(self, exchange, ticker):
    return self.l2_ticker_streams[exchange][ticker]
'''
    
gateway = None
market_data = None
exchanges = ["hyperliquid","paradex"]

trade_fees = {
    'binance': [0.0002, 0.0005], # maker, taker
    'hyperliquid': [0.00015, 0.00045],
    'paradex': [0.00003, 0.0002],
}

maker_register={
    "binance": {},
    "hyperliquid": {},
    "paradex": {},
}

taker_balance = {
    'binance': {},
    'hyperliquid': {},
    'paradex': {},
}

ceil_to_n = lambda x, n: round((math.ceil(x*(10**n)))/(10**n), n)
round_hr = lambda dt: (dt+ timedelta(minutes=30)).replace(second=0, microsecond=0, minute=0)

def usdc_usdt_conversion():
    l2 = market_data.get_l2_stream("binance", "USDCUSDT")
    return l2.get_mid()

async def trade(timeout, **kwargs):
    try:
        await asyncio.wait_for(_trade(**kwargs), timeout=timeout)
    except asyncio.TimeoutError:
        logging.warning(f"Trade for {kwargs.get('asset')} timed out after {timeout} seconds.")

async def _trade(gateway, exchanges, asset, positions, ev_thresholds, per_asset):
    # construct 3 important dicts
    contracts_on_asset = { # exchange > contracts
        exchange: (len(market_data.get_base_mappings(exchange)[asset]) if asset in market_data.get_base_mappings(exchange) else 0) 
        for exchange in exchanges
    }
    
    positions = {exc: pos for exc, pos in zip(exchanges, positions)} # exchange -> positions
    evs= {} # 
    iter = 0
    nominal_exchange = {} # exchange -> nominal held
    skip = set()
    submitted_volumes = {}
    
    while True:
        contracts = [] # list of contract dicts
        for exchange in exchanges:
            mapping = market_data.get_base_mappings(exchange) # exchange > asset > contract
            mapped = mapping.get(asset, []) # returning a list from the key
            
            nominal_position = 0
            
            # Calculating all the relevant metrics for each contract
            for contract in mapped:
                # print("this is the contract")
                # print(contract['symbol'], contract.keys())

                if contract["symbol"] in positions[exchange]:
                    nominal_position += positions[exchange][contract["symbol"]][mk.VALUE]
                    
                lob = market_data.get_l2_stream(exchange, contract['symbol'])
                l2_last = {'ts': lob.timestamp, 'b': lob.bids, 'a': lob.asks}
                fx = 1 # if contract['quote_asset'] == 'USDT' else usdc_usdt_conversion()
                logging.info(f"to be fixed fx: {fx}")
                l2_ts = l2_last['ts']
                bidX = l2_last['b'][0][0] * fx
                askX = l2_last['a'][0][0] * fx
                fr_ts = contract['timestamp']
                fr = float(contract['funding_rate'])
                frint = float(contract['funding_interval'])
                nx_ts = contract['next_funding']
                mark = float(contract['mark_price'])
                markX = mark * fx
                
                _interval_ms = frint * 60 * 60 * 1000
                # TODO: need to implement the nx_ts
                print(f"frint: {frint}, _interval_ms: {_interval_ms}, fr: {fr}, nx_ts: {nx_ts}, fr_ts: {fr_ts}, l2_ts: {l2_ts}, mark: {mark}")
                
                now_ms = int(time.time() * 1000)
                if nx_ts is None or fr_ts is None:
                    elapsed_ms = max(0, now_ms - (fr_ts or now_ms))
                    elapsed_ms = min(elapsed_ms, _interval_ms)
                    accrual = mark * fr * (elapsed_ms / _interval_ms if _interval_ms else 0)
                else:
                    accrual = mark * fr * (_interval_ms - max(0, nx_ts - fr_ts)) / _interval_ms if _interval_ms else 0
                    
                accrualX = accrual * fx
                contracts.append({
                    'symbol': contract['symbol'],
                    'l2_ts': l2_ts,
                    'bidX': bidX,
                    'askX': askX,
                    'markX': markX,
                    'accrualX': accrualX,
                    'exchange': exchange, 
                    'lob': lob,
                    'quantity_precision': contract['quantity_precision'],
                    'min_notional': contract['min_notional'],
                }) 
            nominal_exchange[exchange] = float(nominal_position)       
        
        # Intra-exchange pairs comparison (for short and long position on different exchanges)
        ticker_stats = {}
        for i in range(len(contracts)):
            for j in range(i+1, len(contracts)):
                icontract = contracts[i]
                jcontract = contracts[j]
                if icontract['exchange'] == jcontract['exchange']:
                    continue
                
                # Compute tuple in both directions
                ij_ticker = (icontract['symbol'], icontract['exchange'], jcontract['symbol'], jcontract['exchange'])
                ji_ticker = (jcontract['symbol'], jcontract['exchange'], icontract['symbol'], icontract['exchange'])
                
                if ij_ticker in skip or ji_ticker in skip:
                    continue
                
                l2_delay = int(time.time() *1000) - min(icontract['l2_ts'], jcontract['l2_ts'])
                if l2_delay > 4000:
                    skip.add(ij_ticker)
                    skip.add(ji_ticker)
                    continue
                
                # Calculating the spread between two contracts, adjusted based on the funding accruals
                ij_basis = jcontract['bidX'] - icontract['askX'] 
                ij_basis_adj = ij_basis + (icontract['accrualX'] - jcontract['accrualX'])
                
                ji_basis = icontract['bidX'] - jcontract['askX']
                ji_basis_adj = ji_basis + (jcontract['accrualX'] - icontract['accrualX'])
                
                avg_mark = 0.5 * (icontract['markX'] + jcontract['markX'])
                fees = np.max(
                    trade_fees[icontract['exchange']][0] + trade_fees[jcontract['exchange']][1],
                    trade_fees[jcontract['exchange']][0] + trade_fees[icontract['exchange']][1],  
                )
                
                inertia = fees * avg_mark # cost of execution
                ij_ev = (ij_basis_adj - inertia) / avg_mark
                ji_ev = (ji_basis_adj - inertia) / avg_mark
                
                evs[ij_ticker] = 0.9 *evs[ij_ticker]+ 0.1* ij_ev if ij_ticker in evs else ij_ev
                evs[ji_ticker] = 0.9 *evs[ji_ticker]+ 0.1* ji_ev if ji_ticker in evs else ji_ev
                ticker_stats[ij_ticker] = [icontract, jcontract, avg_mark]
                ticker_stats[ji_ticker] = [jcontract, icontract, avg_mark]
            
        iter += 1
        if iter % 15 == 0:
            await asyncio.sleep(1)
            continue
        
        cap_dollars={}
        # assume nominal held = -500, cap = 1000, -500 1500
        for exchange, nominal_held in nominal_exchange.items(): # calculate capital left for trading on each exchange
            cap_dollars[exchange] = [per_asset[exchange] - nominal_held, per_asset[exchange] + nominal_held]
            cap_dollars[exchange] = np.maximum(cap_dollars[exchange], 0) 
        orders, registers = [], []
        for pair, ev in evs.items():
            if pair in skip:
                continue
            
            pair_threshold = np.mean([
                ev_thresholds[pair[1]][0], # exchange for long, with threshold
                ev_thresholds[pair[3]][1] # exchange for short, with threshold
            ])
            
            if ev < pair_threshold:
                continue
            
            cross_contracts = contracts_on_asset[pair[1]] * contracts_on_asset[pair[3]]
            tradable_notional = min(cap_dollars[pair[1]][0], cap_dollars[pair[3]][1])/ cross_contracts # long exchange, short exchange
            
            pair_stats = ticker_stats[pair]
            
            print(tradable_notional, cross_contracts, pair_stats[2], pair_stats[0]['quantity_precision'], pair_stats[0]['min_notional'])
            
            buy, sell = pair_stats[0], pair_stats[1]
            assert buy['exchange'] == pair[1] and sell['exchange'] == pair[3]
            
            cross_precision = min(int(buy['quantity_precision']), int(sell['quantity_precision']))
            min_notional = max(float(buy['min_notional']), float(sell['min_notional'])) + 10.0
            
            min_volume = round(min_notional / pair_stats[2], cross_precision)
            max_volume = round(tradable_notional/pair_stats[2], cross_precision)
            max_volume = max_volume if pair not in submitted_volumes else max_volume - submitted_volumes[pair]
            if min_volume > max_volume : continue
            
            # Maker and taker fees 
            mt_basis = sell['bidX'] - buy['bidX'] # maker on buy, taker on sell
            tm_basis = sell['askX'] - buy['askX'] # taker on buy, maker on sell
            
            mt_fees = trade_fees[buy['exchange']][0] + trade_fees[sell['exchange']][1]
            tm_fees = trade_fees[buy['exchange']][1] + trade_fees[sell['exchange']][0]
            mt_adj_basis = mt_basis - mt_fees * pair_stats[2]
            tm_adj_basis = tm_basis - tm_fees * pair_stats[2]
            
            side = 'mt' if mt_adj_basis > tm_adj_basis else 'tm' # choose which one is more profitable
            
            if side == 'mt':
                taker_book = sell['lob'].get_bids_buffer()
                maker_book = buy['lob'].get_bids_buffer()
                
                # take the minimum of the volume observed at the top of book at the last 5 samples
                # call this the minimal impacted volume
                taker_miv = np.min(taker_book[-5:0,1])
                
                if min_volume > taker_miv:continue
                
                order_config = {
                    "ticker": pair[0],
                    "amount": min(taker_miv, max_volume),
                    "exc": pair[1],
                    "price": maker_book[-1,1,0],
                    "tif": mk.TIME_IN_FORCE_ALO,
                    "price_match": mk.PRICE_MATCH_QUEUE_1,
                    "reduce_only": False,
                }
                
                orders.append(order_config)
                registers.append({
                    "pair_ticker": pair,
                    "maker": pair[1],
                    "maker_ticker": pair[0],
                    "taker": pair[3],
                    "taker_ticker": pair[2],
                    "taker_precision": int(sell['quantity_precision']),
                    "taker_min": ceil_to_n(float(sell['min_notional'])/ pair_stats[2], int(sell['quantity_precision'])),
                })
            
            if side == 'tm':
                taker_book = buy['lob'].get_asks_buffer()
                maker_book = sell['lob'].get_asks_buffer()
                
                taker_miv = np.min(taker_book[-5:0,1])
                
                if min_volume > taker_miv:continue
                
                order_config = {
                    "ticker": pair[2],
                    "amount": min(taker_miv, max_volume),
                    "exc": pair[3],
                    "price": maker_book[-1,1,0],
                    "tif": mk.TIME_IN_FORCE_ALO,
                    "price_match": mk.PRICE_MATCH_QUEUE_1,
                    "reduce_only": False,
                }
                
                orders.append(order_config)
                registers.append({
                    "pair_ticker": pair,
                    "maker": pair[1],
                    "maker_ticker": pair[0],
                    "taker": pair[3],
                    "taker_ticker": pair[2],
                    "taker_precision": int(sell['quantity_precision']),
                    "taker_min": ceil_to_n(float(sell['min_notional'])/ pair_stats[2], int(sell['quantity_precision'])),
                })
            
        if orders:
            await asyncio.gather(*[
                gateway.executor.limit_order(**order) for order in orders
            ]) # submitting all orders concurrently
            
            oids = await asyncio.gather(*[
                oid_limit(gateway.executor.limit_order, order_config) for order_config in orders
            ]) # get order ids
            
            # register each successful order
            for oid, order, register in zip(oids, orders, registers):
                if oid is not None:
                    pair = register['pair_ticker']
                    maker_register[register['maker']][oid] = register
                    if pair not in submitted_volumes:
                        submitted_volumes[pair] = abs(order['amount'])
                    else:
                        submitted_volumes[pair] += abs(order['amount'])

def binance_fill_handler(gateway):
    '''check binance payload documentation: https://developers.binance.com/docs/binance-spot-api-docs/websocket-api/user-data-stream-requests'''
    async def handler(msg):
        assert msg['e'] == 'executionReport'
        ordstatus = msg['X']
        ordtype = msg['o']
        
        if ordstatus not in ['FILLED', 'PARTIALLY_FILLED']:
            return
        
        if ordtype != "LIMIT":
            return
        
        ticker = msg['s']
        oid = msg['i']
        side = msg['S'].upper()
        fillqty = Decimal(msg['l'])
        fillamt = fillqty * (Decimal('1') if side =='BUY' else Decimal('-1'))
        
        await hedge(oid=oid, exchange="binance", fillamt=fillamt, gateway=gateway)
    return handler
            
def hyperliquid_fill_handler(gateway):
    '''check hyperliquid documentation: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions'''
    async def handler(msg):
        if "isSnapshot" in msg and msg['isSnapshot']:
            return 
        
        fills = msg['fills']
        for fill in fills:
            ticker = fill['coin']
            oid = fill['oid']
            side = fill['side'].upper()
            fillqty = Decimal(fill['sz'])
            fillamt = fillqty * (Decimal('1') if side in ("B", "BUY") else Decimal('-1'))
            await hedge(oid=oid, exchange='hyperliquid', fillamt= fillamt, gateway=gateway)
            
    return handler
                        
def paradex_fill_handler(gateway):
    '''check paradex documentation payload: https://docs.paradex.trade/ws/web-socket-channels/fills/fills'''
    async def handler(msg):
        fills = msg['params']['data']
        ticker = fills['market']
        oid= fills['order_id']
        side= fills['side'].upper()
        fillqty=Decimal(fills['size'])
        fillamt= fillqty * (Decimal('1') if side =='BUY' else Decimal('-1'))
        await hedge(oid=oid, exchange='paradex', fillamt=fillamt, gateway=gateway)
        
    return handler
        
'''Taking a scenario here, let says contract order ==1, and the filled amount is 2.1, if not able 
to hedge this amount. 
- if the filled amount is hedge-able by the trading rule, hedge it. otherwise store inside a buffer
'''
async def hedge(oid, exchange, fillamt, gateway):
    global taker_balance
    try:
        if oid not in maker_register[exchange]:
            return 
        register = maker_register[exchange][oid]
        
        taker = register['taker']
        taker_ticker = register['taker_ticker']
        
        held_balance = taker_balance[taker][taker_ticker] if taker_ticker in taker_balance[taker] else Decimal('0')
        hedge_amount = fillamt + Decimal('-1') + held_balance
        if hedge_amount == 0:
            taker_balance[taker][taker_ticker] = Decimal('0')
            return
        
        taker_precision = register['taker_precision']
        taker_min = register['taker_min']
        if round(hedge_amount, taker_precision) != hedge_amount or abs(hedge_amount) < taker_min:
            taker_balance[taker][taker_ticker] = hedge_amount
            
            logging.warning('Order size has been buffered: hedge amount: %s', hedge_amount)
            
        else:
            await gateway.executor.market_order(
                ticker=taker_ticker,
                amount=hedge_amount,
                exc=taker
            )
            taker_balance[taker][taker_ticker] = Decimal('0')
    except:
        logging.error(f'Error in hedging, exchange {exchange}, {oid}: {fillamt}')

async def oid_limit(afunc, kwargs):
    try:
        res = await afunc(**kwargs)
        
        if kwargs['exc'] == 'binance':
            return res['orderId']
        if kwargs['exc'] == "hyperliquid":
            return res['response']['data']['statuses'][0]['resting']['oid']
        if kwargs['exc'] == "paradex":
            return res['id']
        
        raise KeyError(f"Unknown exchange {kwargs['exc']} for oid extraction.")
    except Exception as e:
        logging.error(f"Error in submitting order {kwargs}: {e}")
        return None

async def hedge_excess(gateway, exchanges, assets):
    global maker_register
    global taker_balance
    
    maker_register = {exc:{} for exc in exchanges}
    taker_balance = {exc: {} for exc in exchanges}
    
    positions = await asyncio.gather(*[gateway.positions.position_get(exc=exchange) for exchange in exchanges])
    asset_positions = collections.defaultdict(list)
    
    for position, exchange in zip(positions, exchanges):
        mapping = market_data.get_base_mappings(exchange)
        
        for asset in assets:
            for contract in mapping[asset]:
                if contract['symbol'] in position:
                    asset_positions[asset].append(position[contract['symbol']])
                    
    asset_net = {}
    for asset, positions in asset_positions.items():
        asset_net[asset] = np.sum([pos[mk.AMOUNT] for pos in positions])
    hedge_orders = []
    
    for asset, net in asset_net.items():
        # TODO: Binance references here (using largest exchange for hedging)
        if net != 0:
            asset_mappings = market_data.get_base_mappings('binance')[asset]
            hedge_contract = None
            
            for contract in asset_mappings:
                if contract['quote_asset'] in ['USDC', 'USDT']:
                    hedge_contract = contract 
                    break
            assert hedge_contract is not None
            excess_hedge = round(net * -1, int(hedge_contract['quantity_precision']))
            
            if excess_hedge == 0:
                continue
            
            if float(abs(excess_hedge)) * hedge_contract['mark_price'] < hedge_contract['min_notional']:
                continue
            
            hedge_order = {
                'ticker': hedge_contract['symbol'],
                'amount': excess_hedge,
                'tif': mk.TIME_IN_FORCE_ALO,
                'price_match': mk.PRICE_MATCH_QUEUE_1,
                'reduce_only': False,
                'exc': 'binance'
            }
            hedge_orders.append(hedge_order)
            
    if hedge_orders:
        await asyncio.gather(*[
            gateway.executor.market_order(**order) for order in hedge_orders
        ])
            
    
    print(asset_positions)
    
    
async def main():
    global gateway, market_data
    calibrate_for = 2
    leverage = 5

    gateway = Gateway(config_keys=get_key())
    await gateway.init_clients()
    
    # exchange fills backlog
    maker_handlers = {
        "hyperliquid": hyperliquid_fill_handler(gateway),
        "paradex": paradex_fill_handler(gateway),
    }
    
    assert all(exchange in maker_handlers for exchange in exchanges)
    await asyncio.gather(*[
        gateway.executor.account_fill_subscribe(exc=exc, handler= maker_handlers[exc]) for exc in exchanges
    ])
    
    market_data = MarketData(
        gateway, 
        exchanges=exchanges, 
        preference_quote=["USDT","USD"]
    )
    
    asyncio.create_task(market_data.serve_exchanges())
    await asyncio.sleep(20)
    
    while True:
        now = datetime.now(pytz.utc)
        nearest_hr = round_hr(now)
        min_dist= (now.timestamp() - nearest_hr.timestamp())/60
        min_dist = abs(min_dist)
        
        if min_dist < calibrate_for:
            await asyncio.sleep(60)
            
        logging.info(f"Current Time: {now}, Nearest Hour: {nearest_hr}, Minutes to Nearest Hour: {min_dist}")
        
        with open('src/assets.txt', 'r') as f:
            assets = [line.strip() for line in f.readlines() if line != '']
        
        try:
            for exc in exchanges:
                balance = market_data.get_balance(exc)
                
            equities = np.array([market_data.get_balance(exc)[mk.ACCOUNT_EQUITY] for exc in exchanges])
            positions = await asyncio.gather(*[
                gateway.positions.positions_get(exc=exchange) for exchange in exchanges
            ])
        except Exception as e:
            logging.error(f"Error fetching balances or positions: {e}")
            await asyncio.sleep(15)
            continue
        
        try: 
            nominals= np.array(
                [float(np.sum(v[mk.VALUE] for v in position.values())) for position in positions]
            )
            
            betas = nominals / equities # first value is exchange equity, second is nominal (2-dimensional array)
            thresholds = [0.00015 + 0.0005 + np.tanh(beta/3 *np.array([1,-1])) for beta in betas]
            ev_thresholds = {exc: thresh for exc, thresh in zip(exchanges, thresholds)}
            
            per_asset = {ex: leverage * eq/ len(market_data.get_base_mappings(ex)) for  ex, eq in zip(exchanges, equities)}
            
            logging.info(f"ev thresholds: {ev_thresholds}, per asset: {per_asset}")
            
            await asyncio.gather(*[
                trade(
                    timeout = 60*calibrate_for,
                    gateway=gateway,
                    exchanges=exchanges,
                    asset=asset,
                    positions= positions, 
                    ev_thresholds=ev_thresholds,
                    per_asset=per_asset,
                ) for asset in assets
            ])
        except Exception as e:
            logging.exception(f"unhandled error: {e}")
            raise
        
        # This finally block is important: to ensure to cancel all the open orders, when the timeout occurred.
        finally:
            while True:
                try:
                    await asyncio.gather(*[
                        gateway.executor.cancel_open_orders(exc= exchange) for exchange in exchanges
                    ])
                    await hedge_excess(gateway= gateway, exchanges=exchanges, assets=assets)
                    break
                except:
                    logging.exception('Error cancelling remaining orders, trying again')
                    await asyncio.sleep(30)
        
    await gateway.cleanup_clients()
    pass

'''scenario
ideal scenario:
binance: order sized of 10, hpl: 1 order size

order submit 20 on hlp, and only 11 filled,
hedge 11 on binance, but is not rounded.
- store in balance, and balance have 11 order
- hpl filled another 9 orders
> then hedge 9 +11 on binance, total

suppose the iteration is complete, where we cancel all open orders, next iteration, we have order of 10 in hpl
fill 10 on hpl
now balance > 10 +11 = 21
'''
if __name__ == "__main__":
    asyncio.run(main())
