# -*- coding: utf-8 -*-
"""
Created on Fri Nov 18 20:56:43 2022

@author: katab
"""

import pandas as pd
import time 
import os
import sys
sys.path.insert(0,r'C:\Users\katab\OneDrive\Documents\DataThesis\Code')
import Utilities as ut

client = ut.twitter_connect()
directory = r'C:\Users\katab\OneDrive\Documents\DataThesis'
start_time = "2019-01-04T00:00:00.000Z"
end_time = "2022-12-14T00:00:00.000Z"
granularity  = "minute"    
tweetfields = ["created_at","id", "source", "text"]


stringtickerdata = {'TSLA':'TESLA OR TSLA',
      'MSFT': 'MSFT OR MICROSOFT',
      'AMC': 'AMC OR AMC Entertainment',
      'OXY': 'OXY OR Occidental Petroleum Corporation OR Occidental Petroleum Corp OR Occidental Petroleum' ,
      'AMD' : 'AMD OR Advanced Micro Devices Inc OR Advanced Micro Devices' ,
      'AAPL': 'AAPL OR Apple Inc'}

i = 0
time.sleep(60*15)
for ticker in stringtickerdata.keys(): 
    for retweet in [0,1]:
        alltweetcount = None
        if retweet ==0:
            querystring = "("+stringtickerdata.get(ticker)+')  -is:retweet lang:en'
            filename = ticker+'tweetcountexrt.csv'
        else : 
            querystring = "("+stringtickerdata.get(ticker)+') lang:en'
            filename = ticker+'tweetcountinrt.csv'
    
        tweetscount = client.get_all_tweets_count(query = querystring, end_time = end_time,granularity= granularity,start_time=start_time)
        tweetcountdata =  pd.DataFrame(tweetscount[0])
        nexttoken = tweetscount[3].get('next_token')
        
        while nexttoken is not None:
            i+=1
            if i == 290 :
                print(ticker+' sleep')
                print(tweetcountdata['end'].min())
                print(tweetcountdata['end'].max())
                i=0
                time.sleep(60*15)
            tweetscountpage = client.get_all_tweets_count(query = querystring,granularity= granularity, end_time = end_time,start_time=start_time, next_token=nexttoken)
            nexttoken = tweetscountpage[3].get('next_token')
            tweetcountdata = tweetcountdata.append(pd.DataFrame(tweetscountpage[0]))
    
            
        tweetcountdata['Ticker'] = stringtickerdata.get(ticker)
        tweetcountdata['retweet'] = 0
        if alltweetcount is None:
            alltweetcount = tweetcountdata
        else:
            alltweetcount = alltweetcount.append(tweetcountdata)
        
            
        alltweetcount.to_csv(os.path.join(directory,filename))
    
    
