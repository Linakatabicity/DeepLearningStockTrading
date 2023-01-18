# -*- coding: utf-8 -*-
"""
Created on Wed Dec 14 18:35:34 2022
Calculate analytic
@author: katab
"""
import os 
import matplotlib.pyplot as plt 
import seaborn as sns
import pandas as pd
import datetime as dt
from datetime import  timedelta
import sys
sys.path.insert(0,r'C:\Users\katab\OneDrive\Documents\DataThesis\Code')
import Utilities as ut
import pandas_ta as pta
import numpy as np


directory = r'C:\Users\katab\OneDrive\Documents\DataThesis'
stocklist = ut.tickersdata().keys()
holidaysexcluded =ut.holidays_to_exclude()


dts =ut.getallperiod()
alldata = None
alldatadescribed = None
for ticker in stocklist:
    temp =  pd.read_csv(os.path.join(directory,ticker+'.csv'))
    #clean up for saturday and outsidehoursdata
    temp['datetime']=pd.to_datetime(temp['datetime'])
    temp = pd.merge(dts,temp,how ='left', left_on ='datetime',right_on = 'datetime')
    
    
    temp = temp.sort_values(by = 'datetime')
    temp =temp.ffill(axis=0)
    temp['day'] = temp['datetime'].dt.day_name()    
    temp['date']= temp['datetime'].dt.date  
    temp['hour']= temp['datetime'].dt.hour   
    
    
    
    tweetinrt=pd.read_csv(os.path.join(directory,ticker+'tweetcountinrt.csv'))
    tweetinrt = tweetinrt.drop(['Unnamed: 0', 'retweet'],axis=1)
    tweetinrt = tweetinrt.set_index(['end', 'start', 'Ticker'])
    tweetinrt.columns = ['tweetcountinrt']

    tweetexrt=pd.read_csv(os.path.join(directory,ticker+'tweetcountexrt.csv'))
    tweetexrt = tweetexrt.drop(['Unnamed: 0', 'retweet'],axis=1)
    tweetexrt = tweetexrt.set_index(['end', 'start', 'Ticker'])
    tweetexrt.columns = ['tweetcountexrt']
    
    tweetcount = tweetinrt.join(tweetexrt,how='left')
    tweetcount = tweetcount.reset_index()
    tweetcount['datetime']=pd.to_datetime(tweetcount['start'] )
    temp['datetime']=pd.to_datetime(temp['datetime'],utc = True)
    
    temp = temp.merge(tweetcount, how = 'left', on = 'datetime')
    
    

    #clean for stock split
    if ticker == 'TSLA':

        for i in  ['open', 'high', 'low', 'close']:
            temp[i] = temp[i].where(temp['date']>=dt.date(2020,8,31),round(temp[i]/5,2))
            temp[i] = temp[i].where(temp['date']>=dt.date(2022,8,25),round(temp[i]/3,2))
    #plot for volume and grap 
    plt.plot(temp.index,temp['close'], color = 'firebrick')
    plt.title(ticker +' Price Evolution')
    plt.xticks(rotation = 45)
    plt.savefig(os.path.join(directory,'Graphs',ticker +' price evolution.png'),bbox_inches="tight")
    plt.clf()

    tempdaygroup = temp[['date','volume']].groupby(by='date').sum().reset_index()
    plt.plot(tempdaygroup['date'],tempdaygroup['volume'], color = 'firebrick')
    plt.title(ticker+ ' Daily Volume')
    plt.xticks(rotation = 45)
    plt.savefig(os.path.join(directory,'Graphs' ,ticker+ ' Daily Volume.png'),bbox_inches="tight")
    plt.clf()
    temp = ut.calculate_analytics(temp)
    temp.to_csv(os.path.join(directory,ticker+'_1m_analytics.csv'))
    
    ##add
    
    
    
    
    temp.set_index('datetime',inplace = True)
    data5min = temp.resample('5T').agg({
                                        "open":  "first",
                                        "high":  "max",
                                        "low":   "min",
                                        "close": "last",
                                        "volume": "sum",
                                        "tweetcountinrt": "sum",
                                        "tweetcountexrt": "sum"
                                    })
    data5min = data5min.dropna()
    data5min = ut.calculate_analytics(data5min)
    data5min.to_csv(os.path.join(directory,ticker+'_5m_analytics.csv'))
    
    data15min = temp.resample('15T').agg({
                                        "open":  "first",
                                        "high":  "max",
                                        "low":   "min",
                                        "close": "last",
                                        "volume": "sum",
                                        "tweetcountinrt": "sum",
                                        "tweetcountexrt": "sum"
                                    })
    data15min = data15min.dropna()
    data15min = ut.calculate_analytics(data15min)
    data15min.to_csv(os.path.join(directory,ticker+'_15m_analytics.csv'))



   
    bins = [-100,-75, -50,-25, 0, 25, 50,75, 100]
    data15min['volume_return_bucket']=  pd.cut(data15min['volume_return'], bins)
    for i in [1,2,3,5,10]:
        data15min['volume_return_lag_'+str(i)] = data15min['volume_return'].shift(i)
        data15min['volume_return_lag_-'+str(i)] = data15min['volume_return'].shift(-i)
        
        data5min['volume_return_lag_'+str(i)] = data5min['volume_return'].shift(i)
        data5min['volume_return_lag_-'+str(i)] = data5min['volume_return'].shift(-i)
        
        temp['volume_return_lag_'+str(i)] = temp['volume_return'].shift(i)
        temp['volume_return_lag_-'+str(i)] = temp['volume_return'].shift(-i)
        
    correlationbuck = data15min.groupby('volume_return_bucket')[['volume_return','tweetcountinrt_return']].corr()
   
    fullcorrelation = data15min[['close_return', 'volume_return',
    'tweetcountinrt_return', 'tweetcountexrt_return',
    'volume_return_lag_1',
    'volume_return_lag_-1', 'volume_return_lag_2', 'volume_return_lag_-2',
    'volume_return_lag_3', 'volume_return_lag_-3', 'volume_return_lag_5',
    'volume_return_lag_-5', 'volume_return_lag_10',
    'volume_return_lag_-10']].corr()
    
       
    fullcorrelation5min = data5min[['close_return', 'volume_return',
    'tweetcountinrt_return', 'tweetcountexrt_return',
    'volume_return_lag_1',
    'volume_return_lag_-1', 'volume_return_lag_2', 'volume_return_lag_-2',
    'volume_return_lag_3', 'volume_return_lag_-3', 'volume_return_lag_5',
    'volume_return_lag_-5', 'volume_return_lag_10',
    'volume_return_lag_-10']].corr()
    
           
    fullcorrelation1min = temp[['close_return', 'volume_return',
    'tweetcountinrt_return', 'tweetcountexrt_return',
    'volume_return_lag_1',
    'volume_return_lag_-1', 'volume_return_lag_2', 'volume_return_lag_-2',
    'volume_return_lag_3', 'volume_return_lag_-3', 'volume_return_lag_5',
    'volume_return_lag_-5', 'volume_return_lag_10',
    'volume_return_lag_-10']].corr()
    
    
    

   
    

    tempdescribe = temp.describe().reset_index()
    tempdescribe['symbol'] = ticker
    print(len(temp))
    if alldata is None:
        alldata = temp
        alldatadescribed = tempdescribe
    else:
        alldata = alldata.append(temp)
        alldatadescribed = alldatadescribed.append(tempdescribe)
        





countobs = alldata[['symbol','close']].groupby(by='symbol').count().reset_index()
plt.bar(countobs['symbol'], countobs['close'])
plt.title('Observations Count per Ticker')
plt.savefig(os.path.join(directory,'Graphs','observationscount.png'),bbox_inches="tight")
plt.clf()
sns.boxplot(x="symbol", y="close", data=alldata, palette="Pastel1", showfliers = False)
plt.title('Price Distribution per Ticker')
plt.savefig(os.path.join(directory,'Graphs','pricedistribution.png'),bbox_inches="tight")
plt.clf()
sns.boxplot(x="symbol", y="volume", data=alldata, palette="Pastel1", showfliers = False)
plt.title('Volume Distribution per Ticker')
plt.savefig(os.path.join(directory,'Graphs','volumedistribution.png'),bbox_inches="tight")
plt.clf()

sns.boxplot(x="symbol", y="volume", data=alldata, palette="Pastel1", showfliers = True)
plt.title('Volume Distribution per Ticker with Outliers')
plt.savefig(os.path.join(directory,'Graphs','volumedistributionincoutliers.png'),bbox_inches="tight")
plt.clf()

sns.boxplot(x="symbol", y="close", data=alldata, palette="Pastel1", showfliers = True)
plt.title('Price Distribution per Ticker')
plt.savefig(os.path.join(directory,'Graphs','pricedistributionoutliers.png'),bbox_inches="tight")
plt.clf()


