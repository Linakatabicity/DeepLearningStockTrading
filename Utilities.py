# -*- coding: utf-8 -*-
"""
Created on Sat Dec 24 17:01:31 2022

@author: katab
"""
def holidays_to_exclude():
    import datetime as dt
    holidaysexcluded =[dt.date(2020,1,1),dt.date(2021,11,25),dt.date(2022,12,15)
                       ,dt.date(2022,11,24),dt.date(2019,11,28),dt.date(2021,1,1)
                       ,dt.date(2021,12,24),dt.date(2020,11,26),dt.date(2020,4,10),
                       dt.date(2021,2,15),dt.date(2022,4,15),dt.date(2022,1,17),
                       dt.date(2020,2,17),dt.date(2021,1,18),dt.date(2019,2,18),
                       dt.date(2019,4,19),dt.date(2021,4,2),dt.date(2019,9,2),
                       dt.date(2020,1,20),dt.date(2022,6,20),dt.date(2019,1,21),
                       dt.date(2022,2,21),dt.date(2019,12,25),dt.date(2020,12,25),
                       dt.date(2020,5,25),dt.date(2019,5,27),dt.date(2020,7,3),
                       dt.date(2022,5,30),dt.date(2021,5,31),dt.date(2019,7,4),
                       dt.date(2022,7,4),dt.date(2021,7,5),dt.date(2022,9,5),
                       dt.date(2021,9,6),dt.date(2020,9,7),dt.date(2019,7,3),
                       dt.date(2019,11,29),dt.date(2019,12,24)]
    return holidaysexcluded

def datetime_range(start, end, delta):
    current = start
    while current < end:
        yield current
        current += delta

def getallperiod(timetrading = True):
    import pandas as pd
    import datetime as dt
    from datetime import  timedelta
    dts =pd.DataFrame ( [dt for dt in 
           datetime_range(dt.datetime(2019, 6,1, 13,30), dt.datetime(2022,5,31,20), 
           timedelta(minutes=1))], columns = ['datetime'])

    dts['weekday'] = dts['datetime'].dt.day_name()  
    dts['dates']= dts['datetime'].dt.date  
    dts = dts[~dts['weekday'].isin(['Saturday','Sunday'])]
    dts = dts[~dts['dates'].isin(holidays_to_exclude())]

    dts.set_index('datetime',inplace = True)
    if timetrading == True:
        dts = dts.between_time('13:30','20:00')
    else:
        dts = dts.between_time('13:00','20:30')
    dts = dts.reset_index().drop(['weekday','dates'],axis=1)
    return dts

def tickersdata():
    stringtickerdata = {'TSLA':'TESLA OR TSLA',
          'MSFT': 'MSFT OR MICROSOFT',
          'AMC': 'AMC OR AMC Entertainment'}
    return stringtickerdata

def twitter_connect():
    import tweepy 
    twitter_api_key =''
    twitter_api_key_secret = ''
    twitter_bearer_token = ''

     #Authentification
    auth = tweepy.OAuthHandler(twitter_api_key, twitter_api_key_secret)
    #auth.set_access_token(twitter_access_token, twitter_access_token_secret)

    client = tweepy.Client(bearer_token=twitter_bearer_token) 
    return client


def calculate_analytics(temp):
    import pandas_ta as pta
    import numpy as np
    #analytics
    #price returns
    for i in ['close','volume','tweetcountinrt','tweetcountexrt']:
        temp[i+'_return']=temp[i].pct_change()*100
        if i =='volume':
            temp['volume_return'] = temp['volume_return'].where(temp['volume_return']<100,100)
            temp['volume_return'] = temp['volume_return'].where(temp['volume_return']>-100,-100)
        temp[i+'_return_30_zscore'] = (temp[i+'_return'] - temp[i+'_return'].rolling(30).mean())/ temp[i+'_return'].rolling(30).std()
        temp[i+'_return_60_zscore'] = (temp[i+'_return'] - temp[i+'_return'].rolling(60).mean())/ temp[i+'_return'].rolling(60).std()
        temp[i+'_return_90_zscore'] = (temp[i+'_return'] - temp[i+'_return'].rolling(90).mean())/ temp[i+'_return'].rolling(90).std()
    
    
    #Exponential moving average ratio
    temp['ewm_30_ratio'] = temp['close']/temp['close'].ewm(span=30).mean()-1
    temp['ewm_60_ratio'] = temp['close']/temp['close'].ewm(span=60).mean()-1
    temp['ewm_90_ratio'] = temp['close']/temp['close'].ewm(span=90).mean()-1
    
    #RSI
    temp['rsi_30'] = pta.rsi(temp['close'], length = 30)/100
    temp['rsi_60'] = pta.rsi(temp['close'], length = 60)/100
    temp['rsi_90'] = pta.rsi(temp['close'], length = 90)/100
    

    temp[['STOCHk_13_8_8', 'STOCHd_13_8_8']]=pta.stoch(temp["high"],temp["low"],temp["close"],13,8,8, append = True)
    temp['STOCHk_13_8_8'] = temp['STOCHk_13_8_8']/100
    temp['STOCHd_13_8_8'] = temp['STOCHd_13_8_8']/100
    temp.replace([np.inf, -np.inf], 0, inplace=True)
    return temp

    

        
        
