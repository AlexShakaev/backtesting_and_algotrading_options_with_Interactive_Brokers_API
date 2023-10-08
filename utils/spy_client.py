# -*- coding: utf-8 -*-
"""
Created on Fri Jul 14 18:19:55 2023

@author: alex shakaev
"""

''' Defines the Client class and its callback methods '''

from threading import Thread, Event
from datetime import datetime

import numpy as np
import pandas as pd 
import pytz
import pyqstrat as pq

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.utils import iswrapper
from ibapi.contract import Contract

spy_con = Contract()
spy_con.symbol = 'SPY'
spy_con.secType = 'STK'
spy_con.exchange = 'SMART'
spy_con.currency = 'USD'

calendar = pq.Calendar.get_calendar(pq.Calendar.NYSE) 
ny = pytz.timezone('America/New_York')

def process_date(date_string):       
    ''' Converts date string to np.datetime64 format'''
    date_string = (date_string.split()[0] + ' ' +  date_string.split()[1])
    date_string = pd.to_datetime(date_string)
    return date_string

class Client(EWrapper, EClient):
    ''' Serves as the client and the wrapper '''

    def __init__(self, addr, port, client_id):
        EWrapper.__init__(self)
        EClient.__init__(self, self)
        self.funds = 0.0
        self.index = 0
        self.entered = False
        self.signal = False        
        self.current_price = 0.0
        self.filled = False                        
        self.expiry = None   
        
        self.spread = {}
        self.data = pd.DataFrame(np.nan, index = range(1), columns = ['date', 'o', 'h', 'l', 'c', 'v', 'diff'])
        self.chain = pd.DataFrame() #columns = ['symbol', 'expiry', 'strike', 'con_id'])
        self.con_data = {}  
        self.con_ids = {}  
        self.bag_bid = {}
        self.bag_ask = {}
        
        self.order_status_df = pd.DataFrame(np.nan, index = range(1), columns = ['orderId', 'status', 'filled', 'remaining', 
                                            'avgFillPrice', 'permId', 'parentId', 'lastFillPrice',
                                            'clientId', 'whyHeld', 'mktCapPrice'])  
        self.open_df = pd.DataFrame(np.nan, index = range(1), columns = [ 'PermId', 'ClientId', 'OrderId', 'Symbol',
                                              'SecType', 'Action', 'OrderType', 'TotalQty', 'CashQty', 'LmtPrice',
                                              'AuxPrice', 'Status'])                 
        self.pos_df = pd.DataFrame(np.nan, index = range(1), columns = [ 'Symbol', 'SecType', 'ConID', 'Expiry',
                                              'Strike', 'Currency', 'Position', 'Avg cost']) 
        self.exec_df = pd.DataFrame(np.nan, index = range(1), columns = ['ReqId', 'PermId', 'Symbol', 'ConID', 'OrderID',
                                                  'SecType', 'Currency', 'ExecId',
                                                  'Time', 'Account', 'Exchange',
                                                  'Side', 'Shares', 'Price',
                                                  'AvPrice', 'cumQty', 'OrderRef'])
        self.comm_df = pd.DataFrame(np.nan, index = range(1), columns = [ 'execId', 'commission', 'currency', 'realizedPNL'])                                                             
        self.pnl_df = pd.DataFrame(np.nan, index = range(1), columns=['ReqId', 'DailyPnL', 'UnrealizedPnL', 'RealizedPnL'])
        self.acc_df = pd.DataFrame(np.nan, index = range(1), columns=['Symbol', 'SecType',  'ConID', 'Expiry', 'Strike', 'Right', 
                                            'position', 'MktPrice','MktValue', 'AvgCost', 'unrealized', 'realized' ])
          
        self.exec_event = Event()
        self.price_event = Event()
        self.chain_event = Event()
        self.account_event = Event()
        self.bag_event = Event()
        self.id_event = Event()
    
        # Connect to TWS
        self.connect(addr, port, client_id)
        
        # Launch the client thread
        thread = Thread(target=self.run)
        thread.start()
    
    @iswrapper
    def nextValidId(self, orderId):
        super().nextValidId(orderId)
        self.nextValidOrderId = orderId 
        self.id_event.set()        
        # print("NextValidId:", orderId)
        
    @iswrapper
    def accountSummary(self, req_id, acct, tag, val, currency):   
        if tag == 'AvailableFunds':
            print('Account {}: available funds = {}'.format(acct, val))
            self.funds = float(val)
    @iswrapper                
    def accountDownloadEnd(self, accountName):
        self.acc_df = self.acc_df.iloc[1:, :]
        self.account_event.set()
        # print("AccountDownloadEnd. Account:", accountName)
    
    @iswrapper
    def updatePortfolio(self, con, position,
                             marketPrice, marketValue,
                             averageCost, unrealizedPNL,
                             realizedPNL, accountName):
        super().updatePortfolio(con, position, marketPrice, marketValue,
                                     averageCost, unrealizedPNL, realizedPNL, accountName)
        acc_info = { 'Symbol' : con.symbol, 'SecType' : con.secType,  'ConID'  : con.conId, 
                        'Expiry'  : con.lastTradeDateOrContractMonth, 'Strike' : con.strike, 'Right' : con.right, 'position' : position, 'MktPrice' : marketPrice,
                      'MktValue' : marketValue, 'AvgCost' : averageCost, 'unrealized' : unrealizedPNL, 
                      'realized' : realizedPNL }       
        if con.conId in self.acc_df.loc[:, 'ConID'].values:
            #drop this row and populate with new info 
            # ind = self.acc_df.index[self.acc_df.loc[:, 'ConID'] == con.conId]
            ind =  self.acc_df.loc[ :,'ConID'].loc[lambda x: x==con.conId].index
            self.acc_df = self.acc_df.drop(self.acc_df.iloc[ind].index, axis = 0)
            self.acc_df = pd.concat([self.acc_df, pd.DataFrame.from_records([acc_info])], ignore_index=True) 
        else:  
            self.acc_df = pd.concat([self.acc_df, pd.DataFrame.from_records([acc_info])], ignore_index=True)
         
    @iswrapper
    def pnl(self, req_id, dailyPnL, unrealizedPnL, realizedPnL):
        super().pnl(req_id, dailyPnL, unrealizedPnL, realizedPnL)
        pnl_info = {"ReqId":req_id, "DailyPnL": dailyPnL, "UnrealizedPnL": unrealizedPnL, "RealizedPnL": realizedPnL}
        self.pnl_df = pd.concat([self.pnl_df, pd.DataFrame.from_records([pnl_info])], ignore_index=True) 
        
    @iswrapper        
    def pnlSingle(self, req_id, pos, dailyPnL,
                        unrealizedPnL, realizedPnL, value):
        super().pnlSingle(req_id, pos, dailyPnL, unrealizedPnL, realizedPnL, value)
            
        print("Daily PnL Single. ReqId:", req_id, "Position:", pos,
                  "DailyPnL:", dailyPnL, "UnrealizedPnL:", unrealizedPnL,
                  "RealizedPnL:", realizedPnL, "Value:", value)
  
    @iswrapper
    def currentTime(self, time):        
        self.time = datetime.fromtimestamp(time, tz = ny)  
        print(f'Current time:{self.time : %Y-%m-%d %X}')
    
    @iswrapper
    def historicalData(self, req_id, bar):   
        hist_bar = { 'date' : bar.date, 'o' : bar.open, 'h' : bar.high, 'l' : bar.low, 'c' : bar.close,
           'v' : bar.volume, 'diff' : ... }
        if bar.date not in self.data['date'].values:
            self.data = pd.concat([self.data, pd.DataFrame.from_records([hist_bar])], ignore_index=True) 
           
    @iswrapper   
    def historicalDataEnd(self, req_id, start, end):        
        self.reqCurrentTime()      
        
        ind = self.data.index[-1]
        self.data = self.data.drop(ind, axis = 0) #incomplete candle 
       
        # Check if price/implied vol is falling
       
        differenced = self.data['c'].diff(1).copy(deep = False)
        self.data['diff'] = differenced*100
                    
        self.data['sma'] = self.data['c'].rolling(window = 50).mean()
        if self.data['sma'].iloc [-1] < self.data['c'].iloc [-1]:
            self.signal = True
        self.data['signal'] = np.where(self.data['sma'] < self.data['c'], 1, 0)   
        
    @iswrapper
    def openOrder( self, order_id, contract, order, state):         
        super().openOrder(order_id, contract, order, state)
        open_order_info = { "PermId": order.permId, "ClientId": order.clientId, "OrderId": order_id, 
                           "Symbol": contract.symbol, "SecType": contract.secType,
                           "Action": order.action, "OrderType": order.orderType,
                          "TotalQty": order.totalQuantity, "CashQty": order.cashQty, 
                          "LmtPrice": order.lmtPrice, "AuxPrice": order.auxPrice, "Status": state.status }       
             
        self.open_df = pd.concat([self.open_df, pd.DataFrame.from_records([open_order_info])], ignore_index=True) 
        print('Status of {} order: {}'.format(contract.symbol, state.status))
         
    @iswrapper
    def orderStatus(self, orderId , status, filled, remaining, avgFillPrice, permId, 
                    parentId, lastFillPrice, clientId, whyHeld, mktCapPrice):
        super().orderStatus(orderId, status, filled, remaining,
            avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice)
        order_status_info =  { 'orderId' : orderId, 'status' : status, 'filled' : filled , 'remaining' : remaining, 
                                        'avgFillPrice' : avgFillPrice, 'permId' : permId, 'parentId' : parentId,
                                        'lastFillPrice' : lastFillPrice, 'clientId' : clientId, 
                                        'whyHeld' : whyHeld, 'mktCapPrice' : mktCapPrice }        
        if status == 'Filled':
            self.filled = True        
            filled_price =  avgFillPrice
            print(f'Order filled at {filled_price}\n')
            
        self.order_status_df = pd.concat([self.order_status_df, pd.DataFrame.from_records([order_status_info])], ignore_index=True)  
                               
    @iswrapper
    def position(self, acct, con, position, avgCost):
        super().position(acct, con, position, avgCost)
        
        pos_info = { "Symbol" : con.symbol, "SecType" : con.secType, "ConID" : con.conId, 
                    "Expiry" : con.lastTradeDateOrContractMonth, "Strike" : con.strike,
                    "Currency" : con.currency, "Position" : position,
                    "Avg cost" : avgCost }       
        self.pos_df = pd.concat([self.pos_df, pd.DataFrame.from_records([pos_info])], ignore_index=True) 
        
    @iswrapper
    def positionEnd(self):
        super().positionEnd()
        print("PositionEnd")
   
    @iswrapper
    def error(self, req_id, errorCode, errorString, advancedOrderRejectJson = ''):        
        self.error_code = errorCode
        self.errorString = errorString   
        if errorCode!=2100:
            print("Error {} {} {}".format(req_id,errorCode,errorString))
    
    @iswrapper
    def contractDetails(self, req_id, details):  
        con_info = { 'symbol' : details.contract.localSymbol, 'expiry' : details.contract.lastTradeDateOrContractMonth,
                    'strike' : details.contract.strike, 'con_id' : details.contract.conId }
             
        self.chain = pd.concat([self.chain, pd.DataFrame.from_records([con_info ])], ignore_index=True)    
    
    @iswrapper   
    def contractDetailsEnd(self, req_id):
        super().contractDetailsEnd(req_id)
        # print("ContractDetailsEnd. ReqId:", req_id)                    
        self.strikes = np.array(self.chain.loc[:, 'strike'].sort_values())
        self.chain_event.set()        
               
    @iswrapper    
    def execDetails(self, req_id, con, execution):
        super().execDetails(req_id, con, execution)
        # print("ExecDetails. ReqId:", req_id, "Symbol:", con.symbol, "SecType:", con.secType, "Currency:", con.currency, execution)
        exec_info = { "ReqId" : req_id, "PermId" : execution.permId, "Symbol" : con.symbol, "ConID" : con.conId, 
                     "OrderID" : execution.orderId,
                     "SecType" : con.secType, "Currency" : con.currency, 
                      "ExecId" : execution.execId, "Time" : execution.time, "Account" : execution.acctNumber,
                      "Exchange" : execution.exchange, "Side" : execution.side, "Shares" : execution.shares,
                      "Price" : execution.price, "AvPrice" : execution.avgPrice, "cumQty" : execution.cumQty,
                      "OrderRef" : execution.orderRef }        
        self.exec_df = pd.concat([self.exec_df, pd.DataFrame.from_records([exec_info])], ignore_index=True)        
        
    @iswrapper
    def execDetailsEnd(self, req_id):
        super().execDetailsEnd(req_id)
        # print("\n\nExecDetailsEnd. ReqId:", req_id)
        self.exec_df.loc[:, 'Time'] =  self.exec_df.loc[:, 'Time'].apply(process_date)
        try:
            self.exec_df.loc[:, 'Time'] =  pd.to_datetime(self.exec_df.loc[:, 'Time']).dt.tz_localize(ny)
        except AttributeError:
            print('Execution dataframe is empty. Please open trades tab in activity monitor')
        #     # self.disconnect()
        self.exec_df = self.exec_df.sort_values(by=['Time'], ascending=True)        
        self.exec_event.set()
            
    @iswrapper
    def commissionReport(self, commissionReport):
        super().commissionReport(commissionReport)
        comm_info = { "execId" : commissionReport.execId, "commission" : commissionReport.commission,
                           "currency" : commissionReport.currency, "realizedPNL" : commissionReport.realizedPNL }
        self.comm_df =  pd.concat([self.comm_df, pd.DataFrame.from_records([comm_info])], ignore_index=True) 
        # print("CommissionReport.", commissionReport)
        
    @iswrapper
    def completedOrder(self, contract, order, orderState):
        print(f'contract {contract},\n order {order},\n orderState {orderState} ')
    
    @iswrapper
    def completedOrdersEnd(self):
        print('Completed orders end! \n')  
	
    @iswrapper            
    def tickPrice(self, req_id, tickType, price, attrib):
        super().tickPrice(req_id, tickType, price, attrib)
        if req_id == 3:
            if tickType == 4:                
                self.current_price = price    
                self.price_event.set()
        if req_id == 11:
            if price != -1 or price != 0.0:
                if tickType == 1:    
                    self.bag_bid [req_id] = price
                elif tickType == 2:
                    self.bag_ask [req_id] = price                    
                self.bag_event.set()
            else:
                print('Invalid price')
                self.bag_event.set()                
                return                                          
 