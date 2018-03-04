import tweepy
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import os

import json

## sanchee's access keys:

access_token = '60239857-QjaNIEDZFta2QGaEtUSrCTN3iRzGXH8IxKBuLtJOo'
access_token_secret =  'nYXlSzIpbzDp1TleRvFk6NEzf9jvB64pMusGdOn8FbLh9'
consumer_key =  'qiOeiMzkmjJmaq9DknJt8w0F2'
consumer_secret =  'zfMYdZa0BW4Td3BlsGm1Owsdc6YYuBb52qevSTYbjA2Fyy1uSw'

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 pyspark-shell'

class StdOutListener(StreamListener):
    def on_data(self, data):
        topic = 'twitter_events_test'
        
        all_data = json.loads(data)
        #print(data)
        #print(" Printing data this time \n\n\n\n\n")
        
        tweets = all_data['text']
        location = all_data['user']['location']
        
    
    
        #producer.send_messages(topic, tweets.encode('utf-8'))
        
        
        print("Printing tweets text this time:")
        print(tweets+"\n\n")
        return True
    def on_error(self, status):
        print (status)


#producer = KafkaProducer(bootstrap_servers='10.0.15.25:6667,10.0.15.26:6667',
#                                 )
        
l = StdOutListener()
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
# api = tweepy.API(auth)
# public_tweets = api.home_timeline()
stream = Stream(auth, l)

#stream.filter(track=['#python','#java','#Analytics'])

stream.filter(track=['python'])