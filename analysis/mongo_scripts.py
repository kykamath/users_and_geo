'''
Created on Oct 3, 2011

@author: kykamath
'''
import sys
from library.file_io import FileIO
sys.path.append('../')
import dateutil.parser
from settings import checkinsFile, venuesFile,\
    minUniqueUsersCheckedInTheLocation, checkinSequenceGraphLocationsFile,\
    lidToNameMapFile
from mongo_settings import checkinsCollection, venuesCollection,\
    locationsCollection, locationToLocationCollection, geoDb, venuesMetaDataCollection,\
    checkinSequenceLocationsCollection
from library.geo import getLidFromLocation, getLocationFromLid,\
    getHaversineDistance
from analysis.mr_analysis import locationIterator, locationGraphIterator,\
    locationByUserDistributionIterator

def addCheckinsToDB():
    i = 0
    for data in open(checkinsFile):
        data = data.strip().split('\t')
        try:
            if len(data)!=7: data.append(None) 
            if len(data) == 7: checkinsCollection.insert({'_id':int(data[1]), 'u': int(data[0]), 'l': [float(data[2]), float(data[3])], 'lid': getLidFromLocation([float(data[2]), float(data[3])]), 't': dateutil.parser.parse(data[4]), 'x': data[5], 'pid': data[6]})
        except Exception as e: print i, 'Exception while processing:', data; i+=1

def addVenuesToDB():
    i = 0
    for data in open(venuesFile):
        data = data.strip().split('\t')
        try:
            venuesCollection.insert({'_id': int(data[0]), 'n': data[1], 'l': [float(data[2]), float(data[3])], 'lid': getLidFromLocation([float(data[2]), float(data[3])]), 'm':' '.join(data[4:-2]), 'tp': int(data[-2]),  'tc': int(data[-1])})
        except Exception as e: print i, 'Exception while processing:', data; i+=1

#def addUserCheckinDistributionToDB():
#    i = 0
#    for data in FileIO.iterateJsonFromFile(userDistributionFile):
#        try:
#            usersCollection.insert({'_id': data['user'], 'tc': data['count'] })
#        except Exception as e: print i, 'Exception while processing:', data; i+=1

def addCheckinSequenceToDB():
    count = 1
    for i in FileIO.iterateJsonFromFile(checkinSequenceGraphLocationsFile):
        title = ''
        lidInfo = venuesCollection.find_one({'lid': i['lid']}, ['n'])
        if lidInfo: title = lidInfo['n']
        print count, len(i['checkins'])
        count+=1
        checkinSequenceLocationsCollection.insert({'_id': i['lid'], 'e': i['edges'], 'c': i['checkins'], 'n': title})

def addVenuesMetaToDB():
    i = 0
    for data in open(venuesFile):
        data = data.strip().split('\t')
#        print data[10].replace('\\', ''), data[11].replace('\\', '')
        try:
            venuesMetaDataCollection.insert({'_id': getLidFromLocation([float(data[2]), float(data[3])]), 'c': data[10].replace('\\', ''), 't':data[11].replace('\\', '') })
        except Exception as e: print i, 'Exception while processing:', data; i+=1

        
def addLocationCheckinDistributionToDB():
    i = 0
    for data in locationIterator(fullRecord=True):
        try:
            locationsCollection.insert({'_id': data['location'], 'l': getLocationFromLid(data['location']), 'tc': data['count'] })
        except Exception as e: print i, 'Exception while processing:', data; i+=1

def addLocationToLocationDistanceToDB():
    i = 0
    for data in locationGraphIterator():
        try:
            d = map(float, data['e'].split())
            d = getHaversineDistance(d[0:2],d[2:])
            locationToLocationCollection.insert({'_id': data['e'], 'u': data['w'], 'd': d})
        except Exception as e: print i, 'Exception while processing:', data; i+=1

def locationToLocationIterator(): return locationToLocationCollection.find()

def writeLidToNameMap():
    for venue in venuesCollection.find():
        FileIO.writeToFileAsJson([venue['lid'], venue['n']], lidToNameMapFile)

if __name__ == '__main__':
#    addCheckinsToDB()
#    addVenuesToDB()
##    addUserCheckinDistributionToDB()
#    addLocationCheckinDistributionToDB()
#    addLocationToLocationDistanceToDB()
#    addVenuesMetaToDB()
#    addCheckinSequenceToDB()
    writeLidToNameMap()