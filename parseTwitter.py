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
    for line in gzip.open(file, 'rb'):
        try:
    #    for tweet in TweetFiles.iterateTweetsFromGzip(file):
    #        tweet = cjson.decode(line)
            data = cjson.decode(line)
            if 'geo' in data and data['geo']!=None:
    #        if 'text' in data: 
    #            yield data
                checkin = {'geo': data['geo']['coordinates'], 'id': data['id'], 'created_at': data['created_at'], 'hashtags': [], 'text': data['text']}
                for h in data['entities']['hashtags']: checkin['hashtags'].append(h['text'])
                print checkin
#                print data['keys']
#                exit()
        except Exception as e:
            print e
            exit()
    exit()
#    for i in os.walk(bdeDataFolder%month):
#        print i