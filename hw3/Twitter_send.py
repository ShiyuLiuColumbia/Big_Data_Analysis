
# coding: utf-8

# In[1]:



CONSUMER_KEY = "#####"
CONSUMER_SECRET = "#####" 
ACCESS_TOKEN = "######" 
ACCESS_TOKEN_SCERET = "#####"


# In[2]:


from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
class listener(StreamListener):
      def on_data(self, data):
        print(data)
        return(True)
      def on_error(self, status):
        print(status)


# In[3]:


def get_tweets():
    url = 'https://stream.twitter.com/1.1/statuses/filter.json'
    query_data = [('language', 'en'), ('track','NBA')]
    query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data]) 
    response = requests.get(query_url, auth=my_auth, stream=True)
    print(query_url, response)
    return response


# In[4]:



def send_tweets_to_spark(http_resp, tcp_connection): 
    for line in http_resp.iter_lines():
        try:
            full_tweet = json.loads(line) 
            tweet_text = full_tweet['text']
            print(tweet_text) 
            tcp_connection.send((tweet_text + '\n').encode())
        except:
            e = sys.exc_info()[0]
            print(e)


# In[5]:


import socket
import sys
import requests
import requests_oauthlib
import json


# In[6]:


my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET,ACCESS_TOKEN, ACCESS_TOKEN_SCERET)


# In[ ]:


TCP_IP = "localhost"
TCP_PORT = 9010
conn = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
print("Waiting for TCP connection...")
conn, addr = s.accept()
print(conn, addr )
print("Connected... Starting getting tweets.") 
resp = get_tweets()
send_tweets_to_spark(resp, conn)

