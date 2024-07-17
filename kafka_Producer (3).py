#!/usr/bin/env python
# coding: utf-8

# In[1]:


pip install kafka-python


# In[2]:


pip install yfinance 


# In[3]:


import pandas as pd
from kafka import KafkaProducer
from time import sleep 
from json import dumps 
import json
import yfinance as yf


# In[4]:


producer= KafkaProducer(bootstrap_servers=["localhost:9092"],value_serializer=lambda x:dumps (x).encode ( 'utf-8'))


# In[5]:


producer.send('yfData', value={})


# In[ ]:


#producer=KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=lambda v: json.dumps(v).encode('utf-8') )
stocks = ["AAPL", "MSFT", "AMZN", "GOOGL", "TSLA", "BABA", "JNJ", "V", "JPM", "PG", "KO", "INTC", "BAC", "DIS","IBM", "GE", "CSCO", "PFE", "XOM", "CVX", "CCEP", "VZ", "PEP", "T", "NFLX", "GS", "HON", "ORCL", "MMM", "MRK", "HD", "CAT", "MCD", "SBUX", "NVDA", "BA", "ACN", "ADBE", "V", "MA", "GM", "F", "CSCO", "IBM", "GE", "PEP", "VZ", "XOM"]
for stock in stocks:
    StockData =yf.download(stock, start="2023-11-30", end="2023-12-31", interval='1wk')
    finance_data = StockData.to_json()
    producer.send('yfData', value={'stocks':stock, 'StockData': finance_data})
   
StockData.head()
#producer= KafkaProducer (bootstrap_servers=['localhost:9092'], value_serializer=lambda x: dumps(x).encode ( 'utf-8'))
#producer.send( 'yfData', value={' stocks': 'StockData' })
#bootstrap_servers='192.168.8.127:9092'
#topic='yfData'

#producer=KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=lambda v: json.dumps(v).encode('utf-8') )

 
# Fetch data and send it to Kafka
#for stock in stocks:
 #   finance_data = StockData.to_json()
  #  producer.send('yfData', value={'stocks':stock, 'StockData': finance_data})
 
# Close the producer


# In[ ]:


#while True:
    
#producer.send('yfData', value={'pecentage_changes'})


# In[ ]:




