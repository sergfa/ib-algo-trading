#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct  7 17:55:23 2020

@author: sfayman
"""
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Sep 27 15:49:46 2020

@author: sfayman
"""
import os
import threading
import time
import algoutils
from trading_app import TradingApp
import pandas as pd
from copy import deepcopy
import numpy as np
from datetime import datetime
import sys
from ibapi.scanner import ScannerSubscription
from ibapi.tag_value import TagValue
from pathlib import Path
import fire


#Port 7497 for paper, 7496 for real
PORT = 7497
HOST = '127.0.0.1'
CLIENT_ID = 2

historicalData = {}
capital = 1000

#tickets = ['BE', 'RDS A', 'HYLN', 'MRVL', 'EEM', 'AMWL', 'LYFT', 'KRE', 'T', 'EWZ', 'EWH', 'USO', 'VEA', 'SCHW', 'AVTR', 'WORK', 'FXI', 'XLF', 'MS', 'GOLD']

#tickets = ['PLUG','REGN','AMD','TSM','HYLN','INFY','GILD','GPRO','CSIQ','DPZ','RXT','HELE','FSLY','CVV','BE','SRAC','PRMW','RUN','SPCE','UMC','ENPH','ATNM','AFIN','CRSP','BC','PKE','ATEC','ZI','SEDG','NBIX']

EXCHANGE = 'ISLAND'

tickets = ['AMD', 'DKNG', 'UAL', 'WKHS', 'NKLA', 'ON', 'INTC', 'CSCO', 'MU', 'LYFT', 'JD', 'MRVL', 'CMCSA', 'RUN', 'PENN', 'GILD', 'VIAC', 'BBBY', 'EBAY', 'OSTK', 'PDD', 'EXC', 'ATVI', 'NNOX', 'WDC', 'AMAT', 'SBUX', 'FITB', 'MRNA', 'MDLZ', 'IQ', 'NLOK', 'KHC', 'CZR', 'WBA', 'AZN', 'GDRX']



def converToDataFrame(data, indexColumn = None):
    df = pd.DataFrame(data);
    if indexColumn is not None:
        df.set_index('Date', inplace=True)
    return df

def get_current_time():
    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")
    return current_time
    
def hot_us_stk(abovePrice, belowPrice, locationCode, scanCode):
    #! [hotusvolume]
    #Hot US stocks by volume
    scanSub = ScannerSubscription()
    scanSub.instrument = "STK"
    scanSub.locationCode = locationCode
    #scanSub.locationCode = "STK.NASDAQ"
    #scanSub.scanCode = "HOT_BY_VOLUME"
    scanSub.scanCode = scanCode
    #scanSub.scanCode = "MOST_ACTIVE"
    scanSub.belowPrice = str(belowPrice)
    scanSub.abovePrice = str(abovePrice)
    return scanSub
    
def get_filter_tags(marketCap, avgVolume):
    tag_values = []
    tag_values.append(TagValue("usdMarketCapAbove", str(marketCap)))
    #tag_values.append(TagValue("optVolumeAbove", "1000"))
    tag_values.append(TagValue("avgVolumeAbove", str(avgVolume)))  
    return tag_values

def fetchHistoricalData(app, tickets, durationStr, barSizeSetting, folder_id = None):
    items = {}
    for index in range(len(tickets)):
        start_time = time.time()
        contract = algoutils.create_contract(tickets[index], exchange=EXCHANGE)
        ticketHistoricalData = app.getHistoricalData(reqId=index,contract=contract, durationStr=durationStr, barSizeSetting=barSizeSetting)
        if ticketHistoricalData is not None:
            df = converToDataFrame(ticketHistoricalData, indexColumn='Date')
            items[tickets[index]] = df
            if folder_id is not None:
                df.to_csv('data/{}/{}_{}_{}.csv'.format(folder_id,tickets[index], durationStr, barSizeSetting), sep='\t')
            print("Data loaded for {}, duration: {} seconds".format(tickets[index], time.time() - start_time))
        else:
            print("Data for {} is None, duration: {} seconds".format(tickets[index], time.time() - start_time))
        time.sleep(5)
            
    return items


def loadHistoricalData(tickets, durationStr, barSizeSetting, folder_id):
    data = {}
    for ticket in tickets:
        file_name = 'data/{}/{}_{}_{}.csv'.format(folder_id, ticket, durationStr, barSizeSetting)
        my_file = Path(file_name)
        if my_file.is_file():
            data_set = pd.read_csv(file_name, sep='\t')
            data_set.set_index('Date', inplace=True)
            data[ticket] = data_set
    return data



def calcIndicators(ohlc_dict):
    histData = deepcopy(ohlc_dict)
    for ticket in ohlc_dict:
        #print("Calculating MACD & Stochastics for ",ticket)
        ticket_data = histData[ticket]
        ticket_data["stoch"] = algoutils.stochOscltr(ticket_data)
        macd_data = algoutils.MACD(ticket_data)
        ticket_data['macd'] = macd_data['MACD']
        ticket_data['signal'] = macd_data['Signal']   
        ticket_data["atr"] = algoutils.atr(ticket_data,60)
        ticket_data.dropna(inplace=True)
    return histData


def calc_historical_ret(ohlc_dict):    
#Identifying Signals and calculating daily return (Stop Loss factored in)
    tickets_signal = {}
    tickets_ret = {}
    trade_count = {}
    for ticket in ohlc_dict:
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

def print_strategy_kpis(ohlc_data):
    # calculating overall strategy's KPIs
    strategy_df = pd.DataFrame()
    for ticket in ohlc_data:
        strategy_df[ticket] = ohlc_data[ticket]["ret"]
        strategy_df["ret"] = strategy_df.mean(axis=1)

    print("CAGR:{}".format(algoutils.CAGR(strategy_df, 26)))
    print("Sharpe {}".format(algoutils.sharpe(strategy_df,0.025, 26)))
    print("MAX DD {}".format(algoutils.max_dd(strategy_df)))

    # vizualization of strategy return
    (1+strategy_df["ret"]).cumprod().plot()


def print_stock_kpis(ohlc_data, trade_count, file_name = None):
    #calculating individual stock's KPIs
    cagr = {}
    sharpe_ratios = {}
    max_drawdown = {}
    for ticket in ohlc_data:
        #print("calculating KPIs for ",ticket)      
        cagr[ticket] =  algoutils.CAGR(ohlc_data[ticket], 26)
        sharpe_ratios[ticket] =  algoutils.sharpe(ohlc_data[ticket],0.025, 26)
        max_drawdown[ticket] =  algoutils.max_dd(ohlc_data[ticket])
    
    KPI_df = pd.DataFrame([cagr,sharpe_ratios,max_drawdown, trade_count],index=["Return","Sharpe Ratio","Max Drawdown", "Trade Count"])
    T = KPI_df.T
    T.sort_values(by=['Return'], inplace=True, ascending=False)
    if file_name is not None:
        T.to_csv(file_name, sep='\t')
    print(T.to_string())
    
    
        
####  MAIN THREAD #################################                                           

close_event = threading.Event()


class StockAPI(object):
    def __init__(self, account, abovePrice=20, belowPrice = 100, locationCode ='STK.NASDAQ', scanCode='MOST_ACTIVE', marketCap=500000000, avgVolume=500000, exchange='ISLAND', strategy_file=None):
        self.abovePrice = abovePrice
        self.belowPrice = belowPrice
        self.locationCode = locationCode
        self.scanCode = scanCode
        self.marketCap = marketCap
        self.avgVolume = avgVolume
        self.exchange=exchange
        self.strategy_file = strategy_file
        self.app = TradingApp(account)
        self.app.connect(HOST, PORT, clientId=CLIENT_ID)
        time.sleep(1)
        run_app_thread = threading.Thread(target=self.__run_app__)
        run_app_thread.start();
        stop_app_thread = threading.Thread(target=self.__stop_app__)
        stop_app_thread.start();
        



    def __run_app__(self):
        self.app.run()

    def __stop_app__(self):
        close_event.wait()
        if close_event.is_set():
            self.app.disconnect()
        

    def __trending__(self):
        scanner = hot_us_stk(belowPrice=self.belowPrice, abovePrice=self.abovePrice, locationCode=self.locationCode, scanCode=self.scanCode)
        filter_tag = get_filter_tags(marketCap=self.marketCap, avgVolume=self.avgVolume)
        hot_stocks = self.app.getScanner(scanner, filter_tag)
        df = converToDataFrame(hot_stocks)
        return df        
        
    def trending(self):
        df = self.__trending__()
        if len(df.columns)==0:
            tickers = []
        else:
            tickers = df['Symbol'].tolist()
        print("Trending stocks params: above price {}, below price {}, location code {}, scan code {}, market cap {}, avg volume {}".format(self.abovePrice,self.belowPrice, self.locationCode, self.scanCode, self.marketCap, self.avgVolume))
        print("Trending tickers: \n", tickers)

    def strategy(self):
        df =self.__trending__()
        if len(df.columns)==0:
            print("Market scanner data was not found, so we will use a predefined tickers list")
        else:
            tickers = df['Symbol'].tolist()
        folder_id =datetime.now().strftime('%Y%m%d')
        dir_to_save = "data/{}".format(folder_id)
        if not os.path.exists(dir_to_save):            
            os.makedirs(dir_to_save)
        histData = fetchHistoricalData(self.app, tickers, '1 M', '15 mins')   
        ohlc_data = calcIndicators(histData)
        trade_count = calc_historical_ret(ohlc_data)
        print_strategy_kpis(ohlc_data)
        print_stock_kpis(ohlc_data, trade_count, self.strategy_file)

    

if __name__ == '__main__':
  fire.Fire(StockAPI)
  time.sleep(5)
  close_event.set()


