
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

from os import listdir
from os.path import isfile, join

for ticker in stocklist:
    if ticker == 'TSLA':
        onlyfiles = [f for f in listdir(os.path.join(directory,'tweets')) if ticker in f]
        fulldata = None
        interval = 15
        for f in onlyfiles: 
            temp = pd.read_csv(os.path.join(directory,'tweets',f))
            temp['created_at'] = pd.to_datetime(temp['created_at'])
            temp = temp.set_index('created_at')
            temp['totalwords'] = temp['text_clean'].str.split().str.len()
            temp = temp[temp['totalwords']>2]
            temp = temp[[  'text_clean', 'polarity', 'subjectivity', 'neg', 'neu', 'pos',
              'compound']]
            temp.drop_duplicates(inplace = True)
            temp = temp.resample(str(interval)+'T').agg({
                                                "text_clean":  "count",
                                                "compound": "mean",
                                                'neg': 'mean', 
                                                'neu': 'mean', 
                                                'pos': 'mean', 
                                            })
            
            if fulldata is None:
                fulldata = temp
            else:
                fulldata = fulldata.append(temp)
                
                
                
        temp = pd.read_csv(os.path.join(directory,ticker + '_'+str(interval)+'m_analytics.csv'))
        temp['datetime'] = pd.to_datetime(temp['datetime'])
        temp = temp.set_index('datetime')
        temp = temp.join(fulldata)
        temp['sentiment_30_zscore'] = (temp['compound'] - temp['compound'].rolling(30).mean())/ temp['compound'].rolling(30).std()
        temp['sentiment_60_zscore'] = (temp['compound'] - temp['compound'].rolling(60).mean())/ temp['compound'].rolling(60).std()
        temp['sentiment_90_zscore'] = (temp['compound'] - temp['compound'].rolling(90).mean())/ temp['compound'].rolling(90).std()
        temp.to_csv(os.path.join(directory,ticker + '_'+str(interval)+'m_analytics.csv'))
        
            
        
            
            
    
