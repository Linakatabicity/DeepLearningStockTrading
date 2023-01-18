# -*- coding: utf-8 -*-
"""
Created on Tue Oct 11 10:37:33 2022

@author: katab
"""

from eodhd import APIClient
import datetime 
import os

Token = ''
api = APIClient(Token)
df = api.get_exchanges()


stocklist = ['TSLA','MSFT','AMC']
for stock in stocklist:
    alldatastock = None
    now = datetime.datetime.now()
    previous = now-datetime.timedelta(days=120)
    dt_string = now.strftime("%Y-%m-%d %H:%M:%S")
    prev_string = previous.strftime("%Y-%m-%d %H:%M:%S")
    for i in range (12):
        stockdata = api.get_historical_data(stock+'.US','1m', prev_string, dt_string)
        
        dt_string = prev_string
        previous = previous-datetime.timedelta(days=120)
        prev_string = previous.strftime("%Y-%m-%d %H:%M:%S")
    
        if alldatastock is None :
            alldatastock = stockdata
        else:
            alldatastock = alldatastock.append(stockdata)

    print (stock)        
    print(alldatastock.index.min())
    print(alldatastock.index.max())
    alldatastock.to_csv(os.path.join(r'C:\Users\katab\OneDrive\Documents\DataThesis',stock+'.csv'))