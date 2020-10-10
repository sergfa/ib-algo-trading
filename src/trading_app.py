#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Sep 27 19:02:38 2020

@author: sfayman
"""

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
import threading


class TradingApp(EWrapper, EClient):
    def __init__(self, account, timeout = 30.0):
        EWrapper.__init__(self)
        EClient.__init__(self, self)
        self.timeout = timeout
        self.account = account
        self.nextAccountSummaryId = 0
        self.nextProfitAndLostId = 0
        self.nextScannerId = 0
        self.historicalDict = {}
        self.accountSummaryDict = {}
        self.orders = []
        self.positions = []
        self.profitAndLost = []
        self.scannerItems = []
        
        
        self.placeOrderEvent = {}
        self.reqOrderIdEvent = threading.Event()
        self.historicalDataEvent = {}
        self.positionsEvent = threading.Event()
        self.openOrdersEvent = threading.Event()
        self.accountSummaryEvent = threading.Event()
        self.profitAndLostEvent = threading.Event()
        self.scannerEvent = threading.Event()
        
        
    
    def error(self, reqId, errorCode, errorString):
       print("Error {} {} {}".format(reqId, errorCode, errorString))
       
    def contractDetails(self, reqId, contractDetails):
        print('reqId {} contract {}'.format(reqId, contractDetails))
        
    def historicalData(self, reqId, bar):
        if reqId not in self.historicalDict:
            self.historicalDict[reqId] = []
        self.historicalDict[reqId].append({'Date': bar.date, 'Open': bar.open, 'High': bar.high, 'Low': bar.low, "Close": bar.close, 'Volume': bar.volume})   
        
    def historicalDataEnd(self, reqId, start, end):
        super().historicalDataEnd(reqId, start, end)
        if self.historicalDataEvent.get(reqId, None) is not None:
            self.historicalDataEvent[reqId].set()
        
    def nextValidId(self, orderId):
        super().nextValidId(orderId)
        self.nextValidOrderId = orderId
        self.reqOrderIdEvent.set()
            
              
    def openOrder(self, orderId, contract, order, orderState):
        super().openOrder(orderId, contract, order, orderState)
        item = {"PermId":order.permId, "ClientId": order.clientId, "OrderId": orderId, 
                      "Account": order.account, "Symbol": contract.symbol, "SecType": contract.secType,
                      "Exchange": contract.exchange, "Action": order.action, "OrderType": order.orderType,
                      "TotalQty": order.totalQuantity, "CashQty": order.cashQty, 
                      "LmtPrice": order.lmtPrice, "AuxPrice": order.auxPrice, "Status": orderState.status}
        self.orders.append(item)
        if self.placeOrderEvent.get(orderId, None) is not None:
            self.placeOrderEvent[orderId].set()
   
    def position(self, account, contract, position, avgCost):
        super().position(account, contract, position, avgCost)
        item = {"Account":account, "Symbol": contract.symbol, "SecType": contract.secType,
                      "Currency": contract.currency, "Position": position, "Avg cost": avgCost}
        self.positions.append(item)
        
    def positionEnd(self):
        super().positionEnd()
        self.positionsEvent.set()
        
    def openOrderEnd(self):
         super().openOrderEnd()
         self.openOrdersEvent.set()
         
    def accountSummary(self, reqId, account, tag, value, currency):
        super().accountSummary(reqId, account, tag, value, currency)
        item = {"Account": account, "Tag": tag, "Value": value, "Currency": currency}
        if reqId not in self.accountSummaryDict:
            self.accountSummaryDict[reqId] = []
        self.accountSummaryDict[reqId].append(item)    
      
    def accountSummaryEnd(self, reqId):
        super().accountSummaryEnd(reqId)
        self.accountSummaryEvent.set()
        
    def scannerData(self, reqId, rank, contractDetails, distance, benchmark, projection, legsStr):
        super().scannerData(reqId, rank, contractDetails, distance, benchmark,projection, legsStr)
        item = {"Symbol": contractDetails.contract.symbol, "Rank": rank, "Distance": distance, "Benchmark": benchmark, "Projection": projection, "LegsStr": legsStr}
        self.scannerItems.append(item)

    def scannerDataEnd(self, reqId):
       super().scannerDataEnd(reqId)
       self.cancelScannerSubscription(reqId)
       self.scannerEvent.set()
       
    def scannerParameters(self, xml):
       super().scannerParameters(xml) 
       file1 = open("params.xml","w")
       file1.write(xml)
       file1.close()

    """ New methods """

    def placeOrderAndWait(self, orderId, contract, order):
        self.placeOrderEvent[orderId] = threading.Event()
        self.placeOrder(orderId, contract, order)
        self.placeOrderEvent[orderId].wait(self.timeout)
        return next((order for order in self.orders if order["OrderId"] == orderId), None)

        
    def getNewOrderId(self):
        self.reqOrderIdEvent.clear()
        self.reqIds(-1)
        self.reqOrderIdEvent.wait(self.timeout)
        return self.nextValidOrderId
        
    def getHistoricalData(self, reqId, contract, durationStr, barSizeSetting):
        self.historicalDataEvent[reqId] = threading.Event()
        
        self.reqHistoricalData(reqId=reqId,contract=contract, endDateTime='', durationStr=durationStr, barSizeSetting=barSizeSetting, whatToShow='ADJUSTED_LAST',
                      useRTH=1,
                      formatDate=1,
                      keepUpToDate=False,
                      chartOptions=[]
                      )
        self.historicalDataEvent[reqId].wait(self.timeout)
        return self.historicalDict.get(reqId, None)
    
    def pnl(self, reqId: int, dailyPnL: float, unrealizedPnL: float, realizedPnL: float):
        super().pnl(reqId, dailyPnL, unrealizedPnL, realizedPnL)
        item = {"DailyPnL": dailyPnL, "UnrealizedPnL": unrealizedPnL, "RealizedPnL": realizedPnL}
        self.profitAndLost.append(item)
        self.profitAndLostEvent.set()
        
    def getPositions(self):
        self.positions = []
        self.positionsEvent.clear()
        self.reqPositions()
        self.positionsEvent.wait(self.timeout)
        return self.positions
    
    def getOpenOrders(self):
        self.orders = []
        self.openOrdersEvent.clear()
        self.reqOpenOrders()
        self.openOrdersEvent.wait(self.timeout)
        return self.orders
    
    def getAccountSummary(self, tag):
        self.accountSummaryEvent.clear()
        self.nextAccountSummaryId += 1
        self.reqAccountSummary(self.nextAccountSummaryId, 'All', tag)
        self.accountSummaryEvent.wait(self.timeout)
        self.cancelAccountSummary(self.nextAccountSummaryId)
        return self.accountSummaryDict.get(self.nextAccountSummaryId, None)

    def getProfitAndLost(self):
        self.profitAndLostEvent.clear()
        self.nextProfitAndLostId += 1
        self.profitAndLost = []
        self.reqPnL(self.nextProfitAndLostId, self.account, "")
        self.profitAndLostEvent.wait(self.timeout)
        self.cancelPnL(self.nextProfitAndLostId)
        return self.profitAndLost
    
    def getScanner(self, subscription, scannerSubscriptionFilterOptions):
        self.scannerEvent.clear()
        self.scannerItems = []
        self.nextScannerId+=1
        self.reqScannerSubscription(self.nextScannerId, subscription, [], scannerSubscriptionFilterOptions)
        self.scannerEvent.wait(self.timeout)
        return self.scannerItems
        
    def clearHistoricalData(self):
        self.historicalDict = {}
       