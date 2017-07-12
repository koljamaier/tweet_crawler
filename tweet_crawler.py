from twarc import Twarc
import json
import yaml

"""
This script crawls brexit tweets via twarc.
The corresponding tweet-ids were provided on http://www.eecs.qmul.ac.uk/~dm303/brexit/
"""

with open("config.yml", 'r') as ymlfile:
    cfg = yaml.load(ymlfile)

for section in cfg:
    print(section)

consumer_key = cfg['twitter']['consumer_key']
consumer_secret = cfg['twitter']['consumer_secret']
access_token = cfg['twitter']['access_token']
access_token_secret = cfg['twitter']['access_token_secret']

def ids():
    for id in open("brexit_tweet_ids.csv"):
        yield id

t = Twarc(consumer_key, consumer_secret, access_token, access_token_secret)
#for tweet in t.search("ferguson"):
#    print(tweet["text"])

#for tweet in t.hydrate(open('brexit_tweet_ids.csv')):
#    print(tweet["text"])


#for tweet in t.hydrate(ids()):
#    print(tweet['text'])

#with open("brexit_tweet_ids.csv", 'r') as f:
    #head = [next(f) for x in range(1000000)]
    #head = f.read()
#print(head)
#print(len(head))


with open('alpha1.txt', 'w') as outfile:
    for tweet in t.hydrate(ids()):
        keys = ["text", "id", "created_at", "favorite_count", "lang", "place", "coordinates", "user", "entities", "geo", "retweeted", "retweet_count"]
        tweet1 = {filter_key:tweet[filter_key] for filter_key in keys}
        values_json = json.dumps(tweet1, sort_keys=True)
        outfile.write(values_json+"\n")
        print(tweet1['text'])

#import csv
#with open('brexit_tweet_ids.csv', 'r') as csvfile:
#    spamreader = csv.reader(csvfile, delimiter=' ', quotechar='|')
#    for row in spamreader:
#            print(', '.join(row))

#with open('tweets.txt', 'w') as outfile:
#    json.dumps(tweets, outfile)
    #outfile.write(tweets)





