'''
Created on Nov 17, 2011

@author: kykamath
'''
import os, gzip, cjson
from library.twitter import TweetFiles

def tweetFilesIterator():
    bdeDataFolder = '/mnt/chevron/bde/Data/TweetData/GeoTweets/2011/%s/%s/'
    for month in range(2, 12):
        for day in range(1, 32):
            tweetsDayFolder = bdeDataFolder%(month, day)
            if os.path.exists(tweetsDayFolder):
                for _, _, files in os.walk(tweetsDayFolder):
                    for file in files: yield tweetsDayFolder+file

for file in tweetFilesIterator():
    print file
#    for line in gzip.open(file, 'rb'):
    for tweet in TweetFiles.iterateTweetsFromGzip(file):
#        tweet = cjson.decode(line)
        print tweet.keys()
    exit()
#    for i in os.walk(bdeDataFolder%month):
#        print i