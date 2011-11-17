'''
Created on Nov 17, 2011

@author: kykamath
'''
import os, gzip, cjson
from library.twitter import TweetFiles
from library.file_io import FileIO
from library.geo import isWithinBoundingBox
from settings import us_boundary

checkinsFile = 'checkins/%s'

def tweetFilesIterator():
    bdeDataFolder = '/mnt/chevron/bde/Data/TweetData/GeoTweets/2011/%s/%s/'
    for month in range(2, 12):
        outputFile = checkinsFile%month
        for day in range(1, 32):
            tweetsDayFolder = bdeDataFolder%(month, day)
            if os.path.exists(tweetsDayFolder):
                for _, _, files in os.walk(tweetsDayFolder):
                    for file in files: yield outputFile, tweetsDayFolder+file

for outputFile, file in tweetFilesIterator():
    print 'Parsing: %s'%file
    for line in gzip.open(file, 'rb'):
        try:
            data = cjson.decode(line)
            if 'geo' in data and data['geo']!=None:
                if isWithinBoundingBox(data['geo']['coordinates'], us_boundary):
                    checkin = {'geo': data['geo']['coordinates'], 'user': {'id': data['user']['id'], 'l': data['user']['location']}, 'id': data['id'], 't': data['created_at'], 'h': [], 'tx': data['text']}
                    for h in data['entities']['hashtags']: checkin['h'].append(h['text'])
                    print checkin
#                FileIO.writeToFileAsJson(checkin, outputFile)
        except Exception as e: print e
    exit()
