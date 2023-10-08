# -*- coding: utf-8 -*-
"""
Created on Wed Oct  4 21:24:43 2023

@author: alex shakaev
"""

import numpy as np
import time  
import pyqstrat as pq
from ibapi.order import Order
from ibapi.contract import Contract, ComboLeg
from ibapi.execution import ExecutionFilter
from utils.spy_client import spy_con

def check_signal(client, con, event):
    ''' Check SPY sma and price to determine signal ''' 
    
    while not event.is_set():
        starttime = time.time()           
        client.reqHistoricalData(2, con, '', '2 D', '2 mins',
            'ADJUSTED_LAST', 1, 1, False, [])
       
        time.sleep(120 - ((time.time() - starttime) % 120.0))
        
def get_exec_info(client, exec_filter = ExecutionFilter()):
    ''' Get execution dataframe '''    
    
    client.exec_df = client.exec_df[0:0]
    client.reqExecutions(2, exec_filter)   
    while not client.exec_event.is_set():
        client.exec_event.wait() 
    client.exec_event.clear()     

def get_trade_details(client, flag = True): 
    ''' Get tradetime, commission and premium from execution dataframe '''  
    
    client.exec_df = client.exec_df[0:0] #clear exec df 
    get_exec_info(client)
    #we need to flip the legs when selling spread         
    l_trade_mask = (client.exec_df.loc[:, 'ConID'] == client.con_ids['long_leg'] if flag 
        else client.exec_df.loc[:, 'ConID'] == client.con_ids['short_leg'])                   
    long_exec_id = client.exec_df[l_trade_mask].iloc[-1]['ExecId']              
    
    s_trade_mask = (client.exec_df.loc[:, 'ConID'] == client.con_ids['short_leg'] if flag
        else client.exec_df.loc[:, 'ConID'] == client.con_ids['long_leg'])
    short_exec_id = client.exec_df[s_trade_mask].iloc[-1]['ExecId']
    
    trade_time = client.exec_df[l_trade_mask].iloc[-1]['Time']      
    df = client.exec_df[client.exec_df.loc[:, 'Time'] == trade_time] 
    premium = np.abs(round(-(df[df.loc[:, 'SecType'] == 'BAG'][ 'AvPrice'].values[0] )*100, 2))
    comm = round(client.comm_df[client.comm_df['execId'] == long_exec_id].loc[:, 'commission'].values[0] 
                  + client.comm_df[client.comm_df['execId'] == short_exec_id].loc[:, 'commission'].values[0], 2)
    
    return (trade_time, comm, premium)      
   
def get_current_price(client):    
    ''' Get current price of SPY '''    
   
    client.reqMktData(3, spy_con, "", False, False, [])    
    while not client.price_event.is_set():
        client.price_event.wait() 
    client.price_event.clear()  
    client.cancelMktData(3)      
    print(f'\nSPY current price - {client.current_price}\n')     

def get_portfolio(client):
    ''' Get account info with current positions '''
    
    client.reqAccountUpdates(True, "")
    while not client.account_event.is_set():
        client.account_event.wait() 
    client.account_event.clear()  
    client.reqAccountUpdates(False, "")
                
def request_chain(client, con):     
    ''' Get option chain for SPY '''        
    
    client.reqContractDetails(3, con)    
    while not client.chain_event.is_set():  
        print('Getting option chain for SPY... \n')
        client.chain_event.wait()     
        print('Received option chain')
    client.chain_event.clear()  
        
def get_legs_info(client):
    ''' Get strikes and expiry '''
    
    if client.expiry == None:
        print('No expiry date')
        return 
    stock_price = round(client.current_price)    
    atm_strike = client.strikes [pq.np_find_closest(client.strikes, stock_price)]
    # no more than 5 dollars wide
    otm_strike = atm_strike + 5
    if otm_strike not in client.chain.loc[:, 'strike']:
        otm_strike -= 1
    client.spread['long_leg'] = atm_strike
    client.spread['short_leg'] = otm_strike
        
    s_mask = client.chain.loc[ :, 'strike'] == otm_strike
    l_mask = client.chain.loc[ :, 'strike'] == atm_strike
    client.con_ids['short_leg'] = client.chain.at[client.chain[s_mask].index[0], 'con_id']
    client.con_ids['long_leg'] = client.chain.at[client.chain[l_mask].index[0], 'con_id']
    print(f"ATM strike - {client.spread['long_leg']} \n"\
               f"OTM strike - {client.spread['short_leg']} \n"\
               f"Expiry     - {client.expiry}")
        
def create_combo(client):
    ''' Create combo contract '''
    
    bag_con = Contract()
    bag_con.symbol = "SPY"
    bag_con.secType = "BAG"
    bag_con.currency = "USD"
    bag_con.exchange = "BOX" 
    bag_con.comboLegs = []
    
    leg1 = ComboLeg()
    leg1.conId = client.con_ids['long_leg'] #long leg  
    leg1.ratio = 1
    leg1.action = "BUY"
    leg1.exchange = "BOX"

    leg2 = ComboLeg()
    leg2.conId = client.con_ids['short_leg'] #short leg  
    leg2.ratio = 1
    leg2.action = "SELL"
    leg2.exchange = "BOX"

    bag_con.comboLegs = []
    bag_con.comboLegs.append(leg1)
    bag_con.comboLegs.append(leg2)
    return bag_con
    
def get_spread_price(client, bag_con):
    ''' Get mid price of spread '''   
    
    bag_price = 0
    while not bag_price > 0:        
        client.reqMktData(11, bag_con, "", False, False, [])    
        while not client.bag_event.is_set():
            # print('Getting spread price... \n')
            client.bag_event.wait()
                    
        client.bag_event.clear()
        bag_price = round((sum([*client.bag_bid.values()] + [*client.bag_ask.values()])/2), 2)  
        client.cancelMktData(11)    
        # print('  Bid   Ask\n' 
        #       f'{client.bag_bid[11]}  {client.bag_ask[11]}\n'
        #       f'Midprice is {bag_price}')   
    return bag_price   

def get_order_id(client):
    ''' Get next valid order id '''
    
    client.id_event.clear()
    client.reqIds(-1)
    while not client.id_event.is_set():              
        client.id_event.wait()     
    client.id_event.clear()  
    return client.nextValidOrderId    
   
def place_order(client, order_id, con, lmt_price, action = "BUY" ):    
    ''' Place combo order '''
           
    order = Order() 
    order.action = action
    order.orderType = "LMT" 
    order.totalQuantity = 1 
    order.lmtPrice = lmt_price
    order.transmit = True # comment this line if you need to manually confirm order in tws
    
    client.placeOrder(order_id, con, order)
    time.sleep(2)