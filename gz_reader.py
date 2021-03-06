import json
import gzip
import glob
import re
from nltk.stem.porter import PorterStemmer
from nltk.corpus import stopwords
from nltk.stem.wordnet import WordNetLemmatizer
import string

"""
This script processes the .gz-output from the twitter collection tool called poultry.
The output is formatted so that dJST can properly process it.
"""

def atoi(text):
    return int(text) if text.isdigit() else text

def natural_keys(text):
    return [atoi(c) for c in re.split('(\d+)', text) ]

def clean(doc):
    stop_free = " ".join([i for i in doc.lower().split() if i not in stop]) # remove stop words
    punc_free = ''.join(ch for ch in stop_free if ch not in exclude)
    normalized = " ".join(stemmer.stem(word) for word in punc_free.split())
    #normalized = " ".join(lemma.lemmatize(word) for word in punc_free.split())
    return normalized

stemmer = PorterStemmer()
stop = set(stopwords.words('english'))
exclude = set(string.punctuation)
exclude.update(["”","“", "’", "-"])
lemma = WordNetLemmatizer()

list_of_files = glob.glob('C:\\Users\\kmr\\PycharmProjects\\tweet_crawler\\poultry_processing\\brexit_folder_hour\\*.gz')
list_of_files.sort(key=natural_keys)
#list_of_files = list_of_files[0:3]

tweets = []
temp_tweets = []
for i, file_name in enumerate(list_of_files):
    with gzip.open(file_name,'rb') as f:
        file_content = f.read()
    temp_tweets.clear()
    gz_parsed=[doc for doc in filter(None, file_content.splitlines())]
    counter = 0
    for tweet in gz_parsed:
        tweet1 = json.loads(tweet)
        # weitere Klausel, um zu überprüfen ob der clean Tweet mehr als 3 Wörter besitzt...
        if (tweet1["lang"] == "en" and counter < 100):
            temp_tweets.append(clean(tweet1["text"]))
            counter += 1
            print(tweet1["text"])
    with open('C:\\Users\\kmr\\PycharmProjects\\tweet_crawler\\output\\%(num)d.dat' % {'num': i + 1}, 'w', encoding='utf-8') as f:
        for j in range(len(temp_tweets)):
            temp_tweets[j] = "d%(num)d " %{'num' : j+1} + temp_tweets[j]
            f.write("%s\n" % str(temp_tweets[j]))
    #tweets.append(temp_tweets[:])

"""
for i in range(len(tweets)):
    with open('C:\\Users\\kmr\\PycharmProjects\\tweet_crawler\\output\\%(num)d.dat' % {'num': i + 1}, 'w', encoding='utf-8') as f:
        for j in range(len(tweets[i])):
            tweets[i][j] = "d%(num)d " %{'num' : j+1} + tweets[i][j]
            f.write("%s\n" % str(tweets[i][j]))
"""
