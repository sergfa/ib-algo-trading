#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Sep 27 19:12:11 2020

@author: sfayman
"""
from ibapi.contract import Contract
from ibapi.order import Order
import numpy as np

def create_contract(symbol, secType = 'STK', currency = 'USD', exchange = 'SMART' ):
    contract = Contract()
    contract.symbol = symbol
    contract.secType = secType
    contract.currency = currency
    contract.exchange = exchange
    return contract


def req_historical_data(app, reqId, contract, durationStr, barSizeSetting):
    app.reqHistoricalData(reqId=reqId,
                      contract=contract, 
                      endDateTime='', 
                      durationStr=durationStr, 
                      barSizeSetting=barSizeSetting,
                      whatToShow='ADJUSTED_LAST',
                      useRTH=1,
                      formatDate=1,
                      keepUpToDate=False,
                      chartOptions=[]
                      )

def create_market_order(action, totalQuantity):
    order = Order()
    order.action = action
    order.orderType = 'MKT'
    order.totalQuantity = totalQuantity
    return order

def create_stop_order(action, totalQuantity, price):
    order = Order()
    order.action = action
    order.orderType = 'STP'
    order.totalQuantity = totalQuantity
    order.auxPrice = price
    return order



def create_buy_limit_order(totalQuantity,lmtPrice):
    order = Order()
    order.action = 'BUY'
    order.orderType = 'LMT'
    order.totalQuantity = totalQuantity
    order.lmtPrice = lmtPrice
    return order




def MACD(sourceDF, fast=12, slow=26, signal=9, type = 'EMA'):
    df = sourceDF.copy()
    if type == 'EMA':
        df['MA_Fast'] = df['Close'].ewm(span=fast, min_periods=fast).mean()
        df['MA_Slow'] = df['Close'].ewm(span=slow, min_periods=slow).mean()
        df['MACD'] = df['MA_Fast'] - df['MA_Slow']
        df['Signal'] = df['MACD'].ewm(span=signal, min_periods=signal).mean()
    elif type == 'SMA':
        df['MA_Fast'] = df['Close'].rolling(fast).mean()
        df['MA_Slow'] = df['Close'].rolling(slow).mean()
        df['MACD'] = df['MA_Fast'] - df['MA_Slow']
        df['Signal'] = df['MACD'].rolling(signal).mean()
    #df.dropna(inplace=True)
    return df
        


def bollBand(sourceDF, n=20):
    "function to calculate Bollinger Band"
    df = sourceDF.copy()
    #df["MA"] = df['close'].rolling(n).mean()
    df["MA"] = df['Close'].ewm(span=n,min_periods=n).mean()
    df["BB_up"] = df["MA"] + 2*df['Close'].rolling(n).std(ddof=0) #ddof=0 is required since we want to take the standard deviation of the population and not sample
    df["BB_dn"] = df["MA"] - 2*df['Close'].rolling(n).std(ddof=0) #ddof=0 is required since we want to take the standard deviation of the population and not sample
    df["BB_width"] = df["BB_up"] - df["BB_dn"]
    df.dropna(inplace=True)
    return df


def atr(sourceDF, n=20):
    "function to calculate True Range and Average True Range"
    df = sourceDF.copy()
    df['H-L']=abs(df['High']-df['Low'])
    df['H-PC']=abs(df['High']-df['Close'].shift(1))
    df['L-PC']=abs(df['Low']-df['Close'].shift(1))
    df['TR']=df[['H-L','H-PC','L-PC']].max(axis=1,skipna=False)
    #df['ATR'] = df['TR'].rolling(n).mean()
    df['ATR'] = df['TR'].ewm(com=n,min_periods=n).mean()
    df_atr = df[['ATR']]
    df_atr.columns = ['ATR']
    return df_atr




def rsi(DF,n=20):
    "function to calculate RSI"
    df = DF.copy()
    df['delta']=df['Close'] - df['Close'].shift(1)
    df['gain']=np.where(df['delta']>=0,df['delta'],0)
    df['loss']=np.where(df['delta']<0,abs(df['delta']),0)
    avg_gain = []
    avg_loss = []
    gain = df['gain'].tolist()
    loss = df['loss'].tolist()
    for i in range(len(df)):
        if i < n:
            avg_gain.append(np.NaN)
            avg_loss.append(np.NaN)
        elif i == n:
            avg_gain.append(df['gain'].rolling(n).mean()[n])
            avg_loss.append(df['loss'].rolling(n).mean()[n])
        elif i > n:
            avg_gain.append(((n-1)*avg_gain[i-1] + gain[i])/n)
            avg_loss.append(((n-1)*avg_loss[i-1] + loss[i])/n)
    df['avg_gain']=np.array(avg_gain)
    df['avg_loss']=np.array(avg_loss)
    df['RS'] = df['avg_gain']/df['avg_loss']
    df['RSI'] = 100 - (100/(1+df['RS']))
    return df['RSI']


def adx(sourceDF,n=20):
    "function to calculate ADX"
    df2 = sourceDF.copy()
    df2['H-L']=abs(df2['High']-df2['Low'])
    df2['H-PC']=abs(df2['High']-df2['Close'].shift(1))
    df2['L-PC']=abs(df2['Low']-df2['Close'].shift(1))
    df2['TR']=df2[['H-L','H-PC','L-PC']].max(axis=1,skipna=False)
    df2['+DM']=np.where((df2['High']-df2['High'].shift(1))>(df2['Low'].shift(1)-df2['Low']),df2['High']-df2['High'].shift(1),0)
    df2['+DM']=np.where(df2['+DM']<0,0,df2['+DM'])
    df2['-DM']=np.where((df2['Low'].shift(1)-df2['Low'])>(df2['High']-df2['High'].shift(1)),df2['Low'].shift(1)-df2['Low'],0)
    df2['-DM']=np.where(df2['-DM']<0,0,df2['-DM'])

    df2["+DMMA"]=df2['+DM'].ewm(span=n,min_periods=n).mean()
    df2["-DMMA"]=df2['-DM'].ewm(span=n,min_periods=n).mean()
    df2["TRMA"]=df2['TR'].ewm(span=n,min_periods=n).mean()

    df2["+DI"]=100*(df2["+DMMA"]/df2["TRMA"])
    df2["-DI"]=100*(df2["-DMMA"]/df2["TRMA"])
    df2["DX"]=100*(abs(df2["+DI"]-df2["-DI"])/(df2["+DI"]+df2["-DI"]))
    
    df2["ADX"]=df2["DX"].ewm(span=n,min_periods=n).mean()

    return df2['ADX']



def stochOscltr(sourceDF,a=20,b=3):
    """function to calculate Stochastics
       a = lookback period
       b = moving average window for %D"""
    df = sourceDF.copy()
    df['C-L'] = df['Close'] - df['Low'].rolling(a).min()
    df['H-L'] = df['High'].rolling(a).max() - df['Low'].rolling(a).min()
    df['%K'] = df['C-L']/df['H-L']*100
    #df['%D'] = df['%K'].ewm(span=b,min_periods=b).mean()
    return df['%K'].rolling(b).mean()
    



def CAGR(DF, factor=1):
    "function to calculate the Cumulative Annual Growth Rate;"
    df = DF.copy()
    #df["ret"]=df["Close"].pct_change()
    df["cum_return"] = (1 + df["ret"]).cumprod()
    #facors 1 day =1, 1 hour = 6.5, 15 mins = 26, 5 mins = 6.5 * 12 , 1 min = 6.5 * 60
    n = len(df)/(252*factor)
    CAGR = (df["cum_return"].tolist()[-1])**(1/n) - 1
    return CAGR


def volatility(DF, factor=1):
    "function to calculate annualized volatility;"
    df = DF.copy()
    #df["ret"]=df["Close"].pct_change()
    vol = df["ret"].std() * np.sqrt(252*factor)
    return vol

def sharpe(DF,rf = 0.05, factor=1):
    "function to calculate sharpe ratio ; rf is the risk free rate"
    sr = (CAGR(DF, factor) - rf)/volatility(DF, factor)
    return sr


def max_dd(DF):
    "function to calculate max drawdown"
    df = DF.copy()
    #df["ret"]=df["Close"].pct_change()
    df["cum_return"] = (1 + df["ret"]).cumprod()
    df["cum_roll_max"] = df["cum_return"].cummax()
    df["drawdown"] = df["cum_roll_max"] - df["cum_return"]
    df["drawdown_pct"] = df["drawdown"]/df["cum_roll_max"]
    max_dd = df["drawdown_pct"].max()
    return max_dd


def winRate(DF):
    "function to calculate win rate of intraday trading strategy"
    df = DF["return"]
    pos = df[df>1]
    neg = df[df<1]
    return (len(pos)/len(pos+neg))*100

def meanretpertrade(DF):
    df = DF["return"]
    df_temp = (df-1).dropna()
    return df_temp[df_temp!=0].mean()

def meanretwintrade(DF):
    df = DF["return"]
    df_temp = (df-1).dropna()
    return df_temp[df_temp>0].mean()

def meanretlostrade(DF):
    df = DF["return"]
    df_temp = (df-1).dropna()
    return df_temp[df_temp<0].mean()

def maxconsectvloss(DF):
    df = DF["return"]
    df_temp = df.dropna(axis=0)
    df_temp2 = np.where(df_temp<1,1,0)
    count_consecutive = []
    seek = 0
    for i in range(len(df_temp2)):
        if df_temp2[i] == 0:
            seek = 0
        else:
            seek = seek + 1
            count_consecutive.append(seek)
    return max(count_consecutive)