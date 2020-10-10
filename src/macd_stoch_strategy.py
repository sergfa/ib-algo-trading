#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Sep 27 15:49:46 2020

@author: sfayman
"""
import threading
import time
import algoutils
from trading_app import TradingApp
import pandas as pd
from copy import deepcopy
import numpy as np
from datetime import datetime
import sys

#Port 7497 for paper, 7496 for real
PORT = 7497
HOST = '127.0.0.1'
CLIENT_ID = 10

historicalData = {}
capital = 1000
EXCHANGE = 'ISLAND'

tickets = ['AMD', 'DKNG', 'UAL', 'WKHS', 'NKLA', 'ON', 'INTC', 'CSCO', 'MU', 'LYFT', 'JD', 'MRVL', 'CMCSA', 'RUN', 'PENN', 'GILD', 'VIAC', 'BBBY', 'EBAY', 'OSTK', 'PDD', 'EXC', 'ATVI', 'NNOX', 'WDC', 'AMAT', 'SBUX', 'FITB', 'MRNA', 'MDLZ', 'IQ', 'NLOK', 'KHC', 'CZR', 'WBA', 'AZN', 'GDRX']

def run_app():
    app.run()

def stop_app():
    print('Waiting to close event')
    close_event.wait()
    if close_event.is_set():
        app.disconnect()
        print('Client {} is disconnecting'.format(CLIENT_ID))
        

def converToDataFrame(data, indexColumn = None):
    df = pd.DataFrame(data);
    if indexColumn is not None:
        df.set_index('Date', inplace=True)
    return df

def fetchPositions(app):
    positions = app.getPositions()
    df = converToDataFrame(positions)
    return df
    
def fetchOpenOrders(app):
    orders = app.getOpenOrders()
    df = converToDataFrame(orders)
    return df
  
def fetchAccountSummary(app, tag):
    summary = app.getAccountSummary(tag)
    df = converToDataFrame(summary)
    print('Account Summary:')
    print(df)
    
    
def fetchHistoricalData(app, tickets, durationStr, barSizeSetting):
    items = {}
    for index in range(len(tickets)):
        start_time = time.time()
        contract = algoutils.create_contract(tickets[index], exchange=EXCHANGE)
        ticketHistoricalData = app.getHistoricalData(reqId=index,contract=contract, durationStr=durationStr, barSizeSetting=barSizeSetting)
        if ticketHistoricalData is not None:
            df = converToDataFrame(ticketHistoricalData, indexColumn='Date')
            items[tickets[index]] = df
            df.to_csv('data/{}_{}_{}.csv'.format(tickets[index], durationStr, barSizeSetting), sep='\t')
            print("Data loaded for {}, duration: {} seconds".format(tickets[index], time.time() - start_time))
        else:
            print("Data for {} is None, duration: {} seconds".format(tickets[index], time.time() - start_time))
        time.sleep(30)
            
    return items
        
def getLiveHistoricalData(app, tickets, duration_in_seconds, durationStr, barSizeSetting):
    start_time = time.time()
    end_time = start_time + duration_in_seconds
    while time.time() <= end_time:
        fetchHistoricalData(app, tickets, durationStr, barSizeSetting)
        time.sleep(30 - (time.time() - start_time)%30)
        app.clearHistoricalData()
    
def placeBuyLimitOrder(app, symbol, quantity, price):
    contract = algoutils.create_contract(symbol, exchange=EXCHANGE)
    order = algoutils.create_buy_limit_order(quantity, price)
    orderId = app.getNewOrderId()
    placedOrder = app.placeOrderAndWait(orderId, contract, order)
    return placedOrder

def saveHistroicalData(data, tickets, durationStr, barSizeSetting):
    for ticket in tickets:
        ticket_data = data[ticket]
        ticket_data.to_csv('data/{}_{}_{}.csv'.format(ticket, durationStr, barSizeSetting), sep='\t')
        
def loadHistoricalData(tickets, durationStr, barSizeSetting):
    data = {}
    for ticket in tickets:
        file_name = 'data/{}_{}_{}.csv'.format(ticket, durationStr, barSizeSetting)
        data_set = pd.read_csv(file_name, sep='\t')
        data_set.set_index('Date', inplace=True)
        data[ticket] = data_set
    return data


def calcIndicators(ohlc_dict, tickets):
    histData = deepcopy(ohlc_dict)
    for ticket in tickets:
        #print("Calculating MACD & Stochastics for ",ticket)
        ticket_data = histData[ticket]
        ticket_data["stoch"] = algoutils.stochOscltr(ticket_data)
        macd_data = algoutils.MACD(ticket_data)
        ticket_data['macd'] = macd_data['MACD']
        ticket_data['signal'] = macd_data['Signal']   
        ticket_data["atr"] = algoutils.atr(ticket_data,60)
        ticket_data.dropna(inplace=True)
    return histData



def calc_historical_ret(ohlc_dict, tickets):    
#Identifying Signals and calculating daily return (Stop Loss factored in)
    tickets_signal = {}
    tickets_ret = {}
    trade_count = {}
    for ticket in tickets:
        trade_count[ticket] = 0
        tickets_signal[ticket] = ""
        tickets_ret[ticket] = [0]
        ticket_data = ohlc_dict[ticket]
        #print("Calculating daily returns for ",ticket)
        for i in range(1,len(ohlc_dict[ticket])):
            if tickets_signal[ticket] == "":
                tickets_ret[ticket].append(0)
                if ticket_data["macd"][i]> ticket_data["signal"][i] and ticket_data["stoch"][i]> 20 and ticket_data["stoch"][i] > ticket_data["stoch"][i-1]:
                       tickets_signal[ticket] = "Buy"
                       trade_count[ticket]+=1
                         
            elif tickets_signal[ticket] == "Buy":
                if ticket_data["Low"][i]<ticket_data["Close"][i-1] - ticket_data["atr"][i-1]:
                    tickets_signal[ticket] = ""
                    trade_count[ticket]+=1
                    tickets_ret[ticket].append(((ticket_data["Close"][i-1] - ticket_data["atr"][i-1])/ticket_data["Close"][i-1])-1)
                else:
                    tickets_ret[ticket].append((ticket_data["Close"][i]/ticket_data["Close"][i-1])-1)
                    
                    
        ticket_data["ret"] = np.array(tickets_ret[ticket])
    return trade_count

def print_strategy_kpis(ohlc_data, tickets):
    # calculating overall strategy's KPIs
    strategy_df = pd.DataFrame()
    for ticket in tickets:
        strategy_df[ticket] = ohlc_data[ticket]["ret"]
        strategy_df["ret"] = strategy_df.mean(axis=1)

    print("CAGR:{}".format(algoutils.CAGR(strategy_df, 26)))
    print("Sharpe {}".format(algoutils.sharpe(strategy_df,0.025, 26)))
    print("MAX DD {}".format(algoutils.max_dd(strategy_df)))

    # vizualization of strategy return
    (1+strategy_df["ret"]).cumprod().plot()


def print_stock_kpis(ohlc_data,tickets, trade_count):
    #calculating individual stock's KPIs
    cagr = {}
    sharpe_ratios = {}
    max_drawdown = {}
    for ticket in tickets:
        #print("calculating KPIs for ",ticket)      
        cagr[ticket] =  algoutils.CAGR(ohlc_data[ticket], 26)
        sharpe_ratios[ticket] =  algoutils.sharpe(ohlc_data[ticket],0.025, 26)
        max_drawdown[ticket] =  algoutils.max_dd(ohlc_data[ticket])
    
    KPI_df = pd.DataFrame([cagr,sharpe_ratios,max_drawdown, trade_count],index=["Return","Sharpe Ratio","Max Drawdown", "Trade Count"])      
    print(KPI_df.T.to_string())


def shouldBuy(df):
    return df["macd"][-1]> df["signal"][-1] and df["stoch"][-1]> 30 and df["stoch"][-1] > df["stoch"][-2]

def findOrderId(ord_df, symbol, action):
    if len(ord_df.columns)!=0 and ((ord_df['Symbol']==symbol) & (ord_df['Action']==action)).any().any():
        return ord_df[(ord_df['Symbol'] == symbol) & (ord_df['Action'] == action)]["OrderId"].sort_values(ascending=True).values[-1]
    else:
        return None
        
        
def placeBuyAndSTP(app, df, ticker, quantity, ord_df):
    prevBuyOrderId = findOrderId(ord_df, ticker, 'BUY')
    if prevBuyOrderId is not None:
        print("Order BUY {} already exist, canceling it...".format(ticker))
        app.cancelOrder(prevBuyOrderId)
    print("Placing buy order for {} quantity {}".format(ticker, quantity))
    contract = algoutils.create_contract(ticker, exchange=EXCHANGE)
    buyOrder = algoutils.create_market_order('BUY', quantity)
    buyOrderId = app.getNewOrderId()
    placedOrder = app.placeOrderAndWait(buyOrderId, contract, buyOrder)
    if placedOrder is not None:
        prevSellOrderId = findOrderId(ord_df, ticker, 'SELL')
        if prevSellOrderId is not None:
            print("Order SELL {} already exist, canceling it...".format(ticker))
            app.cancelOrder(prevSellOrderId)
    
        print("Placing STP order for {} quantity {}".format(ticker, quantity))
        stpOrderId = app.getNewOrderId()
        stpOrder = algoutils.create_stop_order('SELL', quantity, round(df["Close"][-1]-df["atr"][-1],1))
        app.placeOrderAndWait(stpOrderId, contract, stpOrder)
                   
def updateSTPOrder(app, df, ord_df, ticker, quantity):
    prevSellOrderId = findOrderId(ord_df, ticker, 'SELL')
    if prevSellOrderId is not None:
        print("Order SELL {} found, canceling it and creating a new one".format(ticker))
        #ord_id = ord_df[ord_df["Symbol"]==ticker]["OrderId"].sort_values(ascending=True).values[-1]
        app.cancelOrder(prevSellOrderId)
    print("Creating/Updating  STP order for {} quantity {}".format(ticker, quantity))
    stpOrderId = app.getNewOrderId()
    stpOrder = algoutils.create_stop_order('SELL', quantity, round(df["Close"][-1]-df["atr"][-1],1))
    contract = algoutils.create_contract(ticker, exchange=EXCHANGE)
    app.placeOrderAndWait(stpOrderId, contract, stpOrder)

def get_current_time():
    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")
    return current_time
    

def main(app, tickers, iteration):
    pos_df = fetchPositions(app)
    pos_df.drop_duplicates(inplace=True,ignore_index=True) # position callback tends to give duplicate values
    ord_df = fetchOpenOrders(app)
    print("Starting iteration {}, current time {}".format(iteration, get_current_time()))    
    for i in range(len(tickers)):
        ticker = tickers[i]
        print("Proccessing ticker: {}".format(ticker))
        contract = algoutils.create_contract(ticker, exchange=EXCHANGE)
        histData = app.getHistoricalData(reqId=i,contract=contract, durationStr='1 M', barSizeSetting='15 mins')
        if histData is None:
            print("ERROR!!! Historical data is not found for {} ".format(ticker))
            continue
        df = converToDataFrame(histData, indexColumn='Date')
        df["stoch"] = algoutils.stochOscltr(df)
        macd_df =  algoutils.MACD(df)
        df["macd"] = macd_df["MACD"]
        df["signal"] = macd_df["Signal"]
        df["atr"] = algoutils.atr(df,60)
        df.dropna(inplace=True)
        quantity = int(capital/df["Close"][-1])
        if quantity == 0:
            continue
        if len(pos_df.columns)==0 or ticker not in pos_df["Symbol"].tolist():
            if shouldBuy(df):
                placeBuyAndSTP(app, df, ticker, quantity, ord_df)
                   
        elif len(pos_df.columns)!=0 and ticker in pos_df["Symbol"].tolist():
            if pos_df[pos_df["Symbol"]==ticker]["Position"].sort_values(ascending=True).values[-1] == 0:
                if shouldBuy(df):
                    placeBuyAndSTP(app, df, ticker, quantity, ord_df)
            elif pos_df[pos_df["Symbol"]==ticker]["Position"].sort_values(ascending=True).values[-1] > 0:
                old_quantity = pos_df[pos_df["Symbol"]==ticker]["Position"].sort_values(ascending=True).values[-1]
                updateSTPOrder(app,df,ord_df,ticker, old_quantity)
    app.clearHistoricalData()
    
####  MAIN THREAD #################################                                           
ACCOUNT=str(sys.argv[1])
app = TradingApp(ACCOUNT)
app.connect(HOST, PORT, clientId=CLIENT_ID)

close_event = threading.Event()
run_app_thread = threading.Thread(target=run_app)
run_app_thread.start();
time.sleep(1)

#histData = fetchHistoricalData(app, tickets, '1 Y', '15 mins')
#saveHistroicalData(histData, tickets, '1_Y', '15_mins')

#histData = loadHistoricalData(tickets, '1 Y', '15 mins')
#ohlc_data = calcIndicators(histData, tickets)
#trade_count = calc_historical_ret(ohlc_data, tickets)
#print_strategy_kpis(ohlc_data, tickets)
#print_stock_kpis(ohlc_data, tickets, trade_count)


start_time = time.time()
end_time = start_time + 60 * 60 * 5
iteration = 1
while time.time() <= end_time:
    main(app, tickets, iteration)
    sleep_time = 900 - (time.time() - start_time)%900
    print("Iteration {} is complete , current time: {}. Going to wait {} seconds for the next iteration".format(iteration, get_current_time(), sleep_time))
    time.sleep(sleep_time)
    iteration +=1
#close_event.set()

