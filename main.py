# -*- coding: utf-8 -*- 
"""
Created on Fri Jul 14 18:18:13 2023

@author: alex shakaev
"""

# you need to start tws and open trades tab in activity monitor 
# otherwise you'll get empty execution dataframe
# algo trades only one spy spread at a time

import pandas as pd 
import numpy as np
from datetime import datetime, timedelta
import time  
import threading
from functools import partial 
import pyqstrat as pq
import pytz
import sys
import os
import psutil

from ibapi.contract import Contract
from ibapi.execution import ExecutionFilter

from utils.spy_client import Client, spy_con
from utils.bocd import BOCD, constant_hazard, StudentT

from utils.utils import (check_signal, get_exec_info, get_trade_details, get_current_price, get_portfolio,
                   request_chain, get_legs_info, create_combo, get_spread_price, get_order_id, place_order)
 
calendar = pq.Calendar.get_calendar(pq.Calendar.NYSE)    
    
def main():      
    # Create the client and connect to TWS  
    
    client = Client('127.0.0.1', 7497, 0)  # 4002 for gateway, 7497 for tws
    
    print("serverVersion:%s connectionTime:%s" % (client.serverVersion(),
                                                  client.twsConnectionTime()))    
    client.reqCurrentTime()
    time.sleep(1)
    print(client.time)    
    
    # Access available funds
    client.reqAccountSummary(0, 'All', 'AvailableFunds')
    time.sleep(3)
    client.cancelAccountSummary(0)        
    #=============================================================  
    # Convert unaware Datetime to UTC/Eastern timezone aware Datetime
    ny = pytz.timezone('America/New_York')
    today = datetime.today()
    RTH_start = datetime(today.year, today.month, today.day, 9, 30, 0)
    RTH_start = RTH_start.replace(tzinfo=ny)
    RTH_end = RTH_start + timedelta(hours=6, minutes=15)  
    RTH_end = RTH_end.replace(tzinfo=ny)
    
    get_portfolio(client)    
          
    spy_filter = ExecutionFilter()
    spy_filter.symbol = 'SPY'
    spy_filter.secType = ['OPT', 'BAG']
    client.exec_df = client.exec_df[0:0]
    get_exec_info(client, spy_filter)    
    
    # check if we got existing position in spy
    spy_df = client.acc_df[(client.acc_df['Symbol'] == 'SPY') & (client.acc_df['SecType'] == 'OPT')]
    acc = client.acc_df
    trade_time = datetime.now(ny)
    if any(spy_df.loc[:, 'position']): 
        l = acc.loc[:, 'position'].loc[lambda x: x == 1].index
        long_con_id = int(acc.loc[l].ConID.values[0])
        long_cost = acc.loc[l].AvgCost.values[0]
        l_leg_strike = int(acc.loc[l].Strike.values[0])
        s = acc.loc[:, 'position'].loc[lambda x: x == - 1].index
        short_con_id = int(acc.loc[s].ConID.values[0])
        short_cost = acc.loc[s].AvgCost.values[0]
        s_leg_strike = int(acc.loc[s].Strike.values[0])
        premium = round(np.abs(short_cost - long_cost), 2) 
        client.con_ids['long_leg'] = long_con_id
        client.con_ids['short_leg'] = short_con_id
        bag_con = create_combo(client)
        client.spread['short_leg'] = l_leg_strike
        client.spread['long_leg'] = s_leg_strike
        
        #get trade time from exec_df        
        mask = client.exec_df['ConID']==client.con_ids['long_leg']
        if any(mask):            
            df = client.exec_df[mask]            
            trade_time = df.iloc[-1]['Time']           
            client.entered = True  
            print(f'\nGot position in SPY, {l_leg_strike}-{s_leg_strike} call spread \n')
            print(client.acc_df)
        else:
            print('Unable to find contract ids for past trades')  
    
    if datetime.now().weekday() in [5,6]:
        print('\nNon trading day! Disconnecting.')
        client.disconnect()
        return
    #===================================================
    # Find an expiration date just over a month away
    current_date = np.datetime64(datetime.now()).astype('M8[s]')
    expiries = []
    for i in range(9,13):        
        expiries.append(calendar.third_friday_of_month(i, 2023).astype('M8[s]') + np.timedelta64(8, 'h'))
    
    max_expiry = calendar.add_trading_days(current_date, 30) # At least 30 trading days out   
        
    expiry = expiries[np.searchsorted(expiries, max_expiry)] 
    client.expiry = expiry.item().strftime('%Y%m%d')
       
    spy_opt = Contract()
    spy_opt.symbol = "SPY"
    spy_opt.secType = "OPT"
    spy_opt.currency = "USD"
    spy_opt.exchange = "BOX"
    spy_opt.right = "C"
    spy_opt.lastTradeDateOrContractMonth = client.expiry   
    
    get_current_price(client)  
    request_chain(client, spy_opt)                      
    #=============================================================        
    exit_event = threading.Event()
    data_Thread = threading.Thread(target = check_signal, args =(client, spy_con, exit_event))
    data_Thread.start() # starts requesting hist data
    time.sleep(10)
    #=============================================================     
    lambda_ = 150 # initialize 150*150 matrix for bocd algorithm
    alpha = 0.1
    beta = alpha * client.data['c'].rolling(50).var().iloc[-1] 
    kappa = 1
    mu = 0
    bocd = BOCD(partial(constant_hazard, lambda_),
                 StudentT(alpha, beta, kappa, mu), lambda_)                
    
    while True:        
                
        current_time = datetime.now(ny)       
       
        if current_time > RTH_end: 
            print('End of trading day. Disconnecting!')            
            exit_event.set()
            break
        
        try:
            last_value = client.data['diff'].iloc[-1]
        except:
            print('Data is not available yet. Waiting for next candle')
            time.sleep(10)
            continue
        
        print(client.data.iloc[-10:])        
        bocd.update(last_value)    
                                                   
        #exit if spread can be repurchased for more than we paid
        pnl = False
        if client.entered:
            spread_price = get_spread_price(client, bag_con)*100 
            pnl = np.abs(spread_price) >= premium *1.5   
                                  
        if pnl or (bocd.cp_detected and client.data['c'].iloc[-1] < client.data['c'].iloc[-3]) or (current_time - trade_time) / pd.Timedelta(5, "d") > 5:    
            if client.entered: 
                print('Selling spread')
                client.reqAccountUpdates(True, "")
                client.reqGlobalCancel()                
                lmt_price = get_spread_price(client, bag_con)              
             
                order_id = get_order_id(client)                
                print(f'Placing order with lmt price {lmt_price}')
                place_order(client, order_id, bag_con, lmt_price, "SELL") 
                time.sleep(10)    
                
                while not client.filled and (client.con_ids['short_leg'] in client.acc_df.loc[:,'ConID'].values 
                     and (not 
                     (int(client.acc_df[client.acc_df['ConID'] == client.con_ids['short_leg']].loc[:,'position'].values[0]) == 0))):
                    
                    lmt_price = round(lmt_price - 0.01, 2)                    
                    
                    if not client.filled and client.error_code != 104:    
                        print(f'Adjusting to bid... Placing order with lmt price {lmt_price}')
                        place_order(client, order_id, bag_con, lmt_price, "SELL") 
                        time.sleep(15) 
                    else:
                        print("Order is filled")
                        break              
                
                print('Succesfully placed order\n')                                     
                client.entered = False
                client.filled = False          
                trade_time, comm, premium = get_trade_details(client, False)                
                print(f"Sold SPY call spread at {trade_time : %Y-%m-%d %X}, for {premium}, commission - {comm}\n")
                print(client.acc_df)
                bocd.cp_detected = False
                client.reqAccountUpdates(False, "")
                continue  
        
        if client.signal and not client.entered: # place trade            
            print('Buying spread \n')
            client.reqAccountUpdates(True, "")
            get_current_price(client)  
            get_legs_info(client)            
            bag_con = create_combo(client)
            lmt_price = get_spread_price(client, bag_con)    
            
            order_id = get_order_id(client)
            print(f'Placing order with lmt price {lmt_price}')
            place_order(client, order_id, bag_con, lmt_price) 
            time.sleep(10)     
            
            while not client.filled or not (client.con_ids['short_leg'] in client.acc_df.loc[:,'ConID'].values 
                and (int(client.acc_df[client.acc_df['ConID'] == client.con_ids['short_leg']].loc[:,'position'].values[0]) != 0)):
                lmt_price = round(lmt_price + 0.01, 2)                 
               
                if not client.filled and client.error_code != 104:
                    print(f'Adjusting to ask... Placing order with lmt price {lmt_price}')               
                    place_order(client, order_id, bag_con, lmt_price) 
                    time.sleep(15) 
                else:
                    print("Order is filled")
                    break  
                                                     
            print('Succesfully placed order!\n') 
            client.entered = True   
            client.filled = False            
            trade_time, comm, premium = get_trade_details(client)              
            print(f'Bought SPY call spread at {trade_time : %Y-%m-%d %X}, debit - {premium}, commission - {comm}\n')
            print(client.acc_df)
            client.reqAccountUpdates(False, "")
                                    
        end_time = datetime.now(ny) 
        if end_time - current_time < np.timedelta64(2, 'm'):
            string = 'No signal' if not client.signal else ''
            if client.entered and client.signal:
                string = 'Signal detected but already got position'    
            print(string + '\nWaiting for next candle...\n')            
            time.sleep(120 - ((end_time - current_time).total_seconds() % 120.0))            
     
    client.reqAccountUpdates(False, "")  
    client.reqGlobalCancel()
    # Disconnect from TWS
    client.disconnect()
    sys.exit()

if __name__ == '__main__':
    current_system_pid = os.getpid()
    algo = psutil.Process(current_system_pid)
    try:
        main()   
    except KeyboardInterrupt:     
        print("Aborted")         
        algo.terminate()
