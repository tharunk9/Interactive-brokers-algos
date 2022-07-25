# -*- coding: utf-8 -*-
"""
Created on Sun June 14 21:05:45 2022

@author: Tharun
Strategy Name: sma crossover 1.0 Buy only RTH

description: For S&P500, it uses  sma crossover to buy. 
                         Parabolic SAR for fixed profit/ stop
                         
            USE ONLY for Regular trading hours

Notes: Regular trading hours= 0. It implies data from all times
        then days have to be less as the number of observations would increase

"""


# Import libraries
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import pandas as pd
pd.reset_option('all')
import threading
import time
import numpy as np
import talib as ta



class TradeApp(EWrapper, EClient): 
    def __init__(self): 
        EClient.__init__(self, self) 
        self.data = {}
        self.pos_df = pd.DataFrame(columns=['Account', 'Symbol', 'SecType',
                                    'Currency', 'Position', 'Avg cost'])
        self.order_df = pd.DataFrame(columns=['PermId', 'ClientId', 'OrderId',
                                          'Account', 'Symbol', 'SecType',
                                          'Exchange', 'Action', 'OrderType',
                                          'TotalQty', 'CashQty', 'LmtPrice',
                                          'AuxPrice', 'Status'])
        
    def historicalData(self, reqId, bar):
        #print(f'Time: {bar.date}, Open: {bar.open}, Close: {bar.close}')
        if reqId not in self.data:
            self.data[reqId] = [{"Date":bar.date,"Open":bar.open,"High":bar.high,"Low":bar.low,"Close":bar.close,"Volume":bar.volume}]
        else:
            self.data[reqId].append({"Date":bar.date,"Open":bar.open,"High":bar.high,"Low":bar.low,"Close":bar.close,"Volume":bar.volume})

    def nextValidId(self, orderId):
        super().nextValidId(orderId)
        self.nextValidOrderId = orderId 
        print("NextValidId:", orderId)
        
    def position(self, account, contract, position, avgCost):
        super().position(account, contract, position, avgCost)
        dictionary = {"Account":account, "Symbol": contract.symbol, "SecType": contract.secType,
                      "Currency": contract.currency, "Position": position, "Avg cost": avgCost}
        self.pos_df = self.pos_df.append(dictionary, ignore_index=True)
        
    def positionEnd(self):
        print("Latest position data extracted")
        
    def openOrder(self, orderId, contract, order, orderState):
        super().openOrder(orderId, contract, order, orderState)
        dictionary = {"PermId":order.permId, "ClientId": order.clientId, "OrderId": orderId, 
                      "Account": order.account, "Symbol": contract.symbol, "SecType": contract.secType,
                      "Exchange": contract.exchange, "Action": order.action, "OrderType": order.orderType,
                      "TotalQty": order.totalQuantity, "CashQty": order.cashQty, 
                      "LmtPrice": order.lmtPrice, "AuxPrice": order.auxPrice, "Status": orderState.status}
        self.order_df = self.order_df.append(dictionary, ignore_index=True)
        



def snp_futures(symbol, exp_date = "202209", sec_type="FUT",currency="USD",exchange="GLOBEX"):
    contract = Contract()
    contract.symbol = symbol
    contract.secType = sec_type
    contract.currency = currency
    contract.exchange = exchange
    contract.lastTradeDateOrContractMonth = exp_date
    return contract



def histData(req_num,contract,duration,candle_size):
    """extracts historical data"""
    app.reqHistoricalData(reqId=req_num, 
                          contract=contract,
                          endDateTime='',
                          durationStr=duration,
                          barSizeSetting=candle_size,
                          whatToShow='ADJUSTED_LAST',
                          useRTH=0,
                          formatDate=1,
                          keepUpToDate=0,
                          chartOptions=[])	 # EClient function to request contract details


def websocket_con():
    app.run()

app = TradeApp()
app.connect(host='127.0.0.1', port=7496, clientId=23) #port 4002 for ib gateway paper trading/7497 for TWS paper trading/ 7496 for live trading
con_thread = threading.Thread(target=websocket_con, daemon=True)
con_thread.start()

tickers = ["MES"]


###################storing trade app object in dataframe#######################
def dataDataframe(TradeApp_obj,symbols, symbol):
    "returns extracted historical data in dataframe format"
    df = pd.DataFrame(TradeApp_obj.data[symbols.index(symbol)])
    df.set_index("Date",inplace=True)
    return df




def lin_reg(DF, timeperiod=9):
    """function to calculate linear regression"""
    df = DF.copy()
    df['reg'] = ta.LINEARREG(df['Close'], timeperiod=timeperiod)
    df['reg_angle'] = ta.LINEARREG_ANGLE(df['Close'], timeperiod=timeperiod)
    df['reg_slope'] = ta.LINEARREG_SLOPE(df['Close'], timeperiod=timeperiod)
    
    return round(df['reg'] , 3), df['reg_slope']


   
def sarext(DF, startvalue=0.02, offsetonreverse=0,
               accelerationinitlong=0.02, accelerationlong=0.02, accelerationmaxlong=0.20, 
               accelerationinitshort=0.02, accelerationshort=0.02, accelerationmaxshort=0.20):
    
    df = DF.copy()           
               
    df['SAR'] = ta.SAREXT(df['High'], df["Low"],
                startvalue=startvalue, offsetonreverse=offsetonreverse,
               accelerationinitlong=accelerationinitlong, accelerationlong=accelerationlong, accelerationmaxlong=accelerationmaxlong, 
               accelerationinitshort=accelerationinitshort, accelerationshort=accelerationshort, accelerationmaxshort=accelerationmaxshort)
               
  
    df['psar_sign'] = np.where(df['SAR'] > 0, 'up', 'down')  
    df['PSAR'] = abs(df['SAR'])
    
    return round(df['PSAR'] , 3), df['psar_sign']
   
    
   
    

def ema(DF, timeperiod=21):
    """function to calculate Exponential moving avg"""
    df = DF.copy()
    df['ema'] = ta.EMA(df['Close'], timeperiod=timeperiod)

    return round(df['ema'], 3)

def limitOrder(direction,quantity,lmt_price):
    order = Order()
    order.action = direction
    order.orderType = "LMT"
    order.totalQuantity = quantity
    order.lmtPrice = lmt_price
    return order


def marketOrder(direction,quantity):
    order = Order()
    order.action = direction
    order.orderType = "MKT"
    order.totalQuantity = quantity
    order.tif = "GTC"
    
    return order

def stopOrder(direction,quantity,st_price):
    order = Order()
    order.action = direction
    order.orderType = "STP"
    order.totalQuantity = quantity
    order.auxPrice = st_price
    order.outsideRth = True
    order.tif = "GTC"
    return order




def buy_cond(DF, app, ticker):
    
    df = DF.copy()
    quantity = 1
    
    if ((df['reg'][-1] > df["ema"][-1]) and (df['psar_sign'][-1] == "up")): 
         
        buy_price = round(df['reg'][-1]) + 0.5
        app.reqIds(-1)
        time.sleep(2)
        order_id = app.nextValidOrderId
        app.placeOrder(order_id,snp_futures(ticker),limitOrder("BUY",quantity,buy_price))
        time.sleep(3) 
        print("New Buy order placed")        
        
        # Stoploss/profit
           
        psar_stop = (round(df["psar"][-1] * 4) / 4)
        ema_stop = (round(df["ema"][-1] * 4) / 4)
        reg_stop = (round(df['reg'][-1] * 4) / 4)
        
        print("top", psar_stop, ema_stop, reg_stop)
        
        if df['psar_sign'][-1] == "up":
            st_price = psar_stop
            
        elif df['psar_sign'][-1] == "down":
            st_price = reg_stop
        
                
        order_id = app.nextValidOrderId
        app.placeOrder(order_id+1,snp_futures(ticker), stopOrder("SELL",quantity,st_price ))
        time.sleep(3) 
        print("New Buy order placed")            
       
    

def main():
    app.data = {}
    app.pos_df = pd.DataFrame(columns=['Account', 'Symbol', 'SecType',
                            'Currency', 'Position', 'Avg cost'])
    app.order_df = pd.DataFrame(columns=['PermId', 'ClientId', 'OrderId',
                                      'Account', 'Symbol', 'SecType',
                                      'Exchange', 'Action', 'OrderType',
                                      'TotalQty', 'CashQty', 'LmtPrice',
                                      'AuxPrice', 'Status'])
    app.reqPositions()
    time.sleep(2)
    
    pos_df = app.pos_df
    pos_df.drop_duplicates(inplace=True,ignore_index=True) # position callback tends to give duplicate values
    app.reqOpenOrders()
    time.sleep(2)
    
    ord_df = app.order_df
    
    

    for ticker in tickers:
        
        print("starting passthrough for.....",ticker)
        print("Latest data time:", time.strftime("%H:%M:%S", time.localtime(time.time())))
        histData(tickers.index(ticker),snp_futures(ticker),'2 D', '5 mins')
        time.sleep(5)
        
        # Create te dataset
        df = dataDataframe(app,tickers,ticker)
        df["ema"] = ema(df, timeperiod=21)
        df['reg'], df['reg_slope'] = lin_reg(df, timeperiod=9)
        df["psar"], df['psar_sign'] = sarext(df)
        
        df.dropna(inplace=True)
        print(f"Dataset Created: REG {df['reg'][-1]}, EMA: {df['ema'][-1]}, PSAR: {df['psar'][-1]}, {df['psar_sign'][-1]}")
        
        print(f" PSAR: {df['psar'][-10:]}, {df['psar_sign'][-10:]}")
        
        
        quantity = 1
        
        if quantity == 0:
            continue
        
        if len(pos_df.columns)==0:
            
                buy_cond(df, app, ticker)
                print("No positions were there before")
                # print(pos_df.columns)

          
        elif len(pos_df.columns)!=0 and ( ticker not in pos_df["Symbol"].tolist() ):
            
                buy_cond(df, app, ticker)
                print("No positions of this purticular ticker were there before")
                    
         # the ticker name is in traded orders but no positions          
        elif len(pos_df.columns)!=0 and ( ticker in pos_df["Symbol"].tolist()):
            
            
            active_positions = pos_df[pos_df["Symbol"]==ticker]["Position"].sort_values(ascending=True).values[-1]
            print(f"\nActive Positions: {active_positions}")
            if active_positions == 0:
                
                buy_cond(df, app, ticker)
                print("positions of this purticular ticker were there before and no current position")
                time.sleep(2)  
            
            elif active_positions > 0:
                
                # Manage Stop order
                ord_id = ord_df[ord_df["Symbol"]==ticker]["OrderId"].sort_values(ascending=True).values[-1]
                             
                psar_stop = (round(df["psar"][-1] * 4) / 4)
                psar_prev = (round(df["psar"][-2] * 4) / 4)
                ema_stop = (round(df["ema"][-1] * 4) / 4)
                reg_stop = (round(df['reg'][-1] * 4) / 4)
                
                print("\nBottom stops", psar_stop, ema_stop, reg_stop) 
                       
                if psar_stop != psar_prev:
                    app.cancelOrder(ord_id)
                    app.reqIds(-1)
                    time.sleep(2)
                   
                    
                    if df['psar_sign'][-1] == "up":
                        st_price = psar_stop
                        
                    elif df['psar_sign'][-1] == "down":
                        st_price = reg_stop
                    
                    
                    
                    order_id = app.nextValidOrderId
                    app.placeOrder(order_id+2,snp_futures(ticker), stopOrder("SELL", active_positions, st_price ))
                    print(f"\nThe New stop order is updated to : {st_price} \n ")
                    time.sleep(2)            
                        
            
#extract and store historical data in dataframe repetitively
starttime = time.time()
timeout = time.time() + 60*60*23

while time.time() <= timeout:
    main()
    
    mins = 5
    time.sleep((60*mins) - ((time.time() - starttime) % (60*mins)))
    
    
    
    
    
    
    
    
    
    
