'''
Created on Oct 3, 2011

@author: kykamath
'''
import sys
sys.path.append('../')
import dateutil.parser
from settings import checkinsFile, venuesFile
from mongo_settings import checkinsCollection, venuesCollection
from library.geo import getLidFromLocation

def addCheckinsToDB():
#    i = 0
    for data in open(checkinsFile):
        data = data.strip().split('\t')
        if len(data)!=7: data.append(None) 
        if len(data) == 7: checkinsCollection.insert({'_id':int(data[1]), 'u': int(data[0]), 'l': [float(data[2]), float(data[3])], 'lid': getLidFromLocation([float(data[2]), float(data[3])]), 't': dateutil.parser.parse(data[4]), 'x': data[5], 'pid': data[6]})
        else: print 'Problem in line: ', data
#        i+=1
#        if i==1000: break;

def addVenuesToDB():
    i = 0
    for data in open(venuesFile):
        data = data.strip().split('\t')
        try:
            venuesCollection.insert({'_id': int(data[0]), 'n': data[1], 'l': [float(data[2]), float(data[3])], 'lid': getLidFromLocation([float(data[2]), float(data[3])]), 'm':' '.join(data[4:-2]), 'tp': int(data[-2]),  'tc': int(data[-1])})
        except Exception as e: print i, 'Exception while processing:', data; i+=1
#        i+=1
#        if i==1000: break;

if __name__ == '__main__':
#    addCheckinsToDB()
#    addVenuesToDB()
    data = ['309314', 'starbucks - \\', 'legacy &amp; dallas n. tollway', '33.0769', '-96.8207', '', '5760 legacy drive', 'plano', 'tx', '75024', 'coffee shop', 'coffee\\', 'wifi\\', 'barrista', '', 'hentschy', 'carl h.', '29', '404', '1358']
    print {'_id': int(data[0]), 'n': data[1], 'l': [float(data[2]), float(data[3])], 'lid': getLidFromLocation([float(data[2]), float(data[3])]), 'm':' '.join(data[4:-2]), 'tp': int(data[-2]),  'tc': int(data[-1])}
