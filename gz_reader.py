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

# This is needed to sort the .twords files
def natural_keys(text):
    '''
    alist.sort(key=natural_keys) sorts in human order
    http://nedbatchelder.com/blog/200712/human_sorting.html
    (See Toothy's implementation in the comments)
    '''
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

list_of_files = glob.glob('C:\\Users\\kmr\\PycharmProjects\\tweet_crawler\\brexit_folder\\*.gz')
list_of_files.sort(key=natural_keys)
#list_of_files = list_of_files[0:3]

tweets = []
temp_tweets = []
for file_name in list_of_files:
    with gzip.open(file_name,'rb') as f:
        file_content = f.read()
    temp_tweets.clear()
    gz_parsed=[doc for doc in filter(None, file_content.splitlines())]
    for tweet in gz_parsed:
        tweet1 = json.loads(tweet)
        if (tweet1["lang"] == "en"):
            temp_tweets.append(clean(tweet1["text"]))
    tweets.append(temp_tweets[:])

for i in range(len(tweets)):
    with open('C:\\Users\\kmr\\PycharmProjects\\tweet_crawler\\output\\%(num)d.dat' % {'num': i + 1}, 'w', encoding='utf-8') as f:
        for j in range(len(tweets[i])):
            tweets[i][j] = "d%(num)d " %{'num' : j+1} + tweets[i][j]
            f.write("%s\n" % str(tweets[i][j]))

