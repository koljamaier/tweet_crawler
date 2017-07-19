from twarc import Twarc
import json
import yaml

"""
This script crawls brexit tweets via twarc.
The corresponding tweet-ids are provided on http://www.eecs.qmul.ac.uk/~dm303/brexit/
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

keys = ["text", "id", "created_at", "favorite_count", "lang", "place", "coordinates", "user", "entities", "geo", "retweeted", "retweet_count"]
with open('tweets123.txt', 'w') as outfile:
    for tweet in t.hydrate(ids()):
        tweet1 = {filter_key:tweet[filter_key] for filter_key in keys}
        values_json = json.dumps(tweet1, sort_keys=True)
        outfile.write(values_json+"\n")
        print(tweet1['text'])




