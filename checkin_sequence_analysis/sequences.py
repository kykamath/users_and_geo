'''
Created on Nov 11, 2011

@author: kykamath
'''
import sys, time
sys.path.append('../')
from library.classes import GeneralMethods
from library.file_io import FileIO
from mongo_settings import checkinsCollection,\
    checkinSequenceLocationsCollection
from analysis.mr_analysis import filteredUserIterator
from settings import minLocationsTheUserHasCheckedin,\
    minUniqueUsersCheckedInTheLocation, checkinSequenceGraphFile

OUTGOING_EDGE = 'out'
INCOMING_EDGE = 'in'

def writeCheckinSequenceGraphFile():   
    userSet = set([userVector['user'] for userVector in filteredUserIterator(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation, fullRecord = True)])
    count, total = 1, len(userSet)
    for user in userSet:
        print user, count, total
        checkins = [(c['_id'], c['lid'], time.mktime(c['t'].timetuple())) for c in checkinsCollection.find({'u': user})]
        for i in GeneralMethods.getElementsInWindow(checkins, 2): FileIO.writeToFileAsJson([user, i], checkinSequenceGraphFile)
        count+=1

def createLocationFile():
    def getCheckinsForUser(user): return [[checkin['_id'], checkin['lid'], time.mktime(checkin['t'].timetuple())] for checkin in checkinsCollection.find({'u':user})]
    for location in checkinSequenceLocationsCollection.find({'n':{'$regex':'mcdonald'}}):
    #for i in checkinSequenceLocationsCollection.find():
#        print location['_id'], unicode(location['n']).encode('utf-8')
        print location.keys()
        lid = location['_id']
#        print location['e'].keys()
        userCheckins = {}
        users = set([edge[0]['u'] for type in [OUTGOING_EDGE, INCOMING_EDGE] for edge in location['e'][type]])
        for user in users: 
            if user not in userCheckins: userCheckins[user] = getCheckinsForUser(user)
        location['users'] = userCheckins
        print location.keys()
        exit()

if __name__ == '__main__':
#    writeCheckinSequenceGraphFile()
    createLocationFile()
