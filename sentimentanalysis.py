# -*- coding: utf-8 -*-
"""


@author: katab
"""

from textblob import TextBlob
import os 
import matplotlib.pyplot as plt 
import seaborn as sns
import pandas as pd
import datetime as dt
from datetime import  timedelta
import sys
sys.path.insert(0,r'C:\Users\katab\OneDrive\Documents\DataThesis\Code')
import Utilities as ut
from wordcloud import WordCloud, STOPWORDS
from PIL import Image
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from langdetect import detect
from nltk.stem import SnowballStemmer
from sklearn.feature_extraction.text import CountVectorizer
import re
import nltk
nltk.download('vader_lexicon')
nltk.download('stopwords')
from nltk.corpus import stopwords
import time 
import numpy as np


def minutesbucket(df):
    if df['minutes'] <15:
        return '0-15'
    elif df['minutes'] <30:
        return '15-30'
    elif df['minutes'] <45:
        return '30-45'
    else : 
        return '45+'
    
    

directory = r'C:\Users\katab\OneDrive\Documents\DataThesis'

stocklist = ['TSLA']

holidaysexcluded =ut.holidays_to_exclude()
alltweetdata = None
for ticker in stocklist:
    
    temp=pd.read_csv(os.path.join(r'C:\Users\katab\OneDrive\Documents\DataThesis',ticker+'tweetcountinrt.csv'))
    temp = temp.drop(['Unnamed: 0', 'retweet'],axis=1)
    temp = temp.set_index(['end', 'start', 'Ticker'])
    temp.columns = ['tweetcountinrt']

    temp2=pd.read_csv(os.path.join(r'C:\Users\katab\OneDrive\Documents\DataThesis',ticker+'tweetcountexrt.csv'))
    temp2 = temp2.drop(['Unnamed: 0', 'retweet'],axis=1)
    temp2 = temp2.set_index(['end', 'start', 'Ticker'])
    temp2.columns = ['tweetcountexrt']
    
    temp = temp.join(temp2,how='left')
    temp = temp.reset_index()

    temp['TickerCode'] = ticker
    temp['datetime']=pd.to_datetime(temp['start'])
    temp = temp.sort_values(by ='datetime')
    temp['weekday'] = temp['datetime'].dt.day_name()  
    temp['dates']= temp['datetime'].dt.date  
    temp['hour']= temp['datetime'].dt.hour  
    temp['minutes']= temp['datetime'].dt.minute
    
    temp = temp[~temp['weekday'].isin(['Saturday','Sunday'])]
    temp = temp[~temp['dates'].isin(holidaysexcluded)]
    temp.set_index('datetime',inplace = True)
    temp = temp.between_time('13:00','20:30')
    temp = temp.reset_index()
    temp['minutebracket'] = np.where(temp['minutes']<30,'0-29','29-59')

    tempdaygroup = temp[['dates','tweetcountexrt','tweetcountinrt']].groupby(by='dates').sum().reset_index()
    plt.plot(tempdaygroup['dates'],tempdaygroup['tweetcountexrt'], color = 'skyblue', label ='ex retweet')
    plt.plot(tempdaygroup['dates'],tempdaygroup['tweetcountinrt'], color = 'lightgrey',label ='inc retweet' )
    plt.legend()
    plt.title(ticker+ ' Daily Tweet Count')
    plt.xticks(rotation = 45)
    plt.savefig(os.path.join(directory,'Graphs' ,ticker+ ' Daily Tweet Count.png'),bbox_inches="tight")
    plt.clf()
    
  
    
    if alltweetdata is None:
        alltweetdata = temp
    else:
        alltweetdata = alltweetdata.append(temp)
    

alltweetdata['quarterminutesbuck'] = alltweetdata.apply(minutesbucket, axis =1)

alltweetdata['maxresults'] = alltweetdata['tweetcountexrt'].where(alltweetdata['tweetcountexrt']<500,500)
alltweetdata['datehour'] = pd.to_datetime(alltweetdata['dates']).dt.strftime("%m/%d/%Y")+alltweetdata['hour'].astype(str)
alltweetdata['dateminutes'] = pd.to_datetime(alltweetdata['dates']).dt.strftime("%m/%d/%Y")+alltweetdata['hour'].astype(str) + alltweetdata['minutebracket'] 
alltweetdata['dateminutes2'] = pd.to_datetime(alltweetdata['dates']).dt.strftime("%m/%d/%Y")+alltweetdata['hour'].astype(str) + alltweetdata['quarterminutesbuck'] 


temp = alltweetdata[['datehour','end','start','tweetcountexrt','tweetcountinrt','maxresults']].groupby(by=['datehour']).agg(
    {'end':'max',
     'start':'min',
     'tweetcountexrt' : 'sum',
     'tweetcountinrt':'sum',
     'maxresults':'sum'}).reset_index()


listfilter= temp[temp['tweetcountexrt']>500]['datehour'].drop_duplicates()

request =  temp[temp['tweetcountexrt']<=500][['end', 'start', 'tweetcountexrt', 'tweetcountinrt',
       'maxresults']]

temp = alltweetdata[alltweetdata['datehour'].isin(listfilter)]
temp = temp[['dateminutes','end','start','tweetcountexrt','tweetcountinrt','maxresults']].groupby(by=['dateminutes']).agg(
    {'end':'max',
     'start':'min',
     'tweetcountexrt' : 'sum',
     'tweetcountinrt':'sum',
     'maxresults':'sum'}).reset_index()


listfilter= temp[temp['tweetcountexrt']>500]['dateminutes'].drop_duplicates()

request =request.append(temp[temp['tweetcountexrt']<=500][['end', 'start', 'tweetcountexrt', 'tweetcountinrt',
       'maxresults']])




temp = alltweetdata[alltweetdata['dateminutes'].isin(listfilter)]

temp = temp[['dateminutes2','end','start','tweetcountexrt','tweetcountinrt','maxresults']].groupby(by=['dateminutes2']).agg(
    {'end':'max',
     'start':'min',
     'tweetcountexrt' : 'sum',
     'tweetcountinrt':'sum',
     'maxresults':'sum'}).reset_index()



listfilter= temp[temp['tweetcountexrt']>500]['dateminutes2'].drop_duplicates()
request =request.append(temp[temp['tweetcountexrt']<=500][['end', 'start', 'tweetcountexrt', 'tweetcountinrt',
       'maxresults']])


request =request.append( alltweetdata[alltweetdata['dateminutes2'].isin(listfilter)][['end', 'start', 'tweetcountexrt', 'tweetcountinrt',
       'maxresults']])

request['datetime']=pd.to_datetime(request['start'])
request['dates']= request['datetime'].dt.date 
request['maxresults'] = request['tweetcountexrt'].where(request['tweetcountexrt']<500,500)

request=request.sort_values(by = 'start')





remove_rt = lambda x: re.sub('RT @\w+: ',' ',x)
rt = lambda x: re.sub("(@[A-Za-z0–9]+)|(\w+:\/\/\S+)|([^\w\s])|(\d+)",' ',x)


client = ut.twitter_connect()
tweetfields = ["created_at", "text"]

stopwords = nltk.corpus.stopwords.words("english")

for dates in request['dates'].drop_duplicates():
    temp2 = request[request['dates'] == dates]
    tweetsentiment = None
    
    for testrecord in temp2.to_records():
        print(testrecord)
        resultstest = client.search_all_tweets(r'(TESLA OR TSLA) -is:retweet lang:en', 
                                               tweet_fields  = ["created_at", "text"], 
                                               start_time  = testrecord['start'],
                                               end_time  = testrecord['end'],
                                               max_results = min(max(10,testrecord['maxresults']),500),
                                               sort_order ='relevancy' )
        
        start_time = time.time()


        if resultstest.meta['result_count']>0:
            for tweets in resultstest.data:
                tweetdata = pd.DataFrame([tweets])
                tweetdata['text_clean']  = tweetdata['text'].map(remove_rt).map(rt)
                tweetdata['text_clean']= tweetdata['text_clean'].str.lower()
                tweetdata['text_clean'] = ' '.join([w for w in tweetdata['text_clean'].values[0].split() if w.lower() not in stopwords])
                tweetdata[['polarity', 'subjectivity']] = tweetdata['text_clean'].apply(lambda Text: pd.Series(TextBlob(Text).sentiment))
                tweetdata[['neg', 'neu','pos','compound']]=pd.DataFrame.from_dict(SentimentIntensityAnalyzer().polarity_scores(tweetdata['text_clean'].values[0]),orient='index').transpose()
                if tweetsentiment is None:
                    tweetsentiment = tweetdata
                else:
                    tweetsentiment = tweetsentiment.append(tweetdata)
        if time.time() - start_time <3 :
            time.sleep(3-(time.time() - start_time))
    tweetsentiment.to_csv(os.path.join(directory,'tweets',ticker+dates.strftime("%Y%m%d")+'.csv'))







