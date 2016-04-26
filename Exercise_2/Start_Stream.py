
# coding: utf-8

# In[ ]:

#Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from azure.servicebus import ServiceBusService
import json
from collections import Counter
import urllib3
import sys


key_value = 'TsPSqYRxHLwMfuZW6C+sMWFlX/NuDDfUzhVT/qQNneM=' # SharedAccessKey from Azure portal
key_name = 'manage' # SharedAccessKeyName from Azure portal
sbs = ServiceBusService('w205Ex2twitter',
                        shared_access_key_name=key_name,
                        shared_access_key_value=key_value)
sbs.create_event_hub('w205ex2twitter')

#Variables that contains the user credentials to access Twitter API 
access_token = "723321852928487424-6DpOhp2IVLdLHcT5xqOyp3ZF6KfD045"
access_token_secret = "k1kTDSQke2oivdYwFOwS0cP6bTC4TLK0nM4qpkxhs3tjQ"
consumer_key = "UazvoPbRP6S6X890BWVk9srj5"
consumer_secret = "6McwA8RuB2ZWoDpYP0TpOsqOp9I0PZRfZoesJfLwFfoGw28QKz"

def word_count(tweet):
    # Split the tweet into words
    words = tweet.split()
    # Filter out the hash tags, RT, @ and urls
    valid_words = []
    for word in words:

        # Filter the hash tags
        if word.startswith("#"): continue

        # Filter the user mentions
        if word.startswith("@"): continue

        # Filter out retweet tags
        if word.startswith("RT"): continue

        # Filter out the urls
        if word.startswith("http"): continue
        word = word.replace(",","")
        word = word.replace(".","")
        word = word.replace("-","")
        word = word.replace("~","")
        word = word.replace("`","")
        word = word.replace("!","")
        word = word.replace("?","")
        word = word.replace("_","")
        word = word.replace(":","")
        word = word.replace(";","")
        word = word.replace("^","")
        word = word.replace("+","")
        word = word.replace("<","")
        word = word.replace(">","")
        word = word.replace("/","")
        word = word.replace("\\","")
        # Strip leading and lagging punctuations
        aword = word.strip("\"?><,\'.:!;)")
        
        # now check if the word contains only ascii
        if len(aword) > 0:
            valid_words.append(aword.lower())

    word_counts_array = Counter(valid_words).items()
    return_string = '['
    for word_count_pair in word_counts_array:
        word = word_count_pair[0]
        count = word_count_pair[1]
        return_string = return_string + '{"word":"' +word + '", "count":'+ str(count) + '},'

    return (return_string[:-1] + ']').encode("utf-8", errors='remove')


#look at only the US. This bounding box will be used 
bounding_box = [-124.848974, 24.396308, -66.885444, 49.384358]

#This is a basic listener that just prints received tweets to stdout.
class StdOutListener(StreamListener):

    def on_data(self, data):        
        jsn_data = json.loads(data)
        if 'text' in jsn_data:
            print ''
            print ''
            print "---- RAW TWEET DATA ---- " 
            print jsn_data
            print ''
            print "------ TWEET TEXT ------" 
            print jsn_data['text'].encode(sys.stdout.encoding, errors='replace')
            print ''
            print "------ WORD COUNT ------" 
            print word_count(jsn_data['text'])
            sbs.send_event('w205ex2twitter',word_count(jsn_data['text']).encode("utf-8", errors='remove'))

        return True

    def on_error(self, status):
        print status


if __name__ == '__main__':

    #This handles Twitter authetification and the connection to Twitter Streaming API
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    
    # stream = Stream(auth, l)
    # stream.sample()
    # stream.filter(languages=['en'], locations = bounding_box)   
      
    # to deal with incomplete reads. Automatically restart. BE CAREFUL this automatically re-starts all errors!
    def start_stream():
        while True:
            try:
                stream = Stream(auth, l)
                stream.filter(languages=['en'], locations = bounding_box)   
                #stream.sample(languages=['en'])         
            except KeyboardInterrupt:
                break  
            except:
                continue

    start_stream()