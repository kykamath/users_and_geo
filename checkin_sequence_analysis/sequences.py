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
    for location in checkinSequenceLocationsCollection.find({'n':{'$regex':'mcdonald'}}):
    #for i in checkinSequenceLocationsCollection.find():
#        print location['_id'], unicode(location['n']).encode('utf-8')
        print location.keys()
        lid = location['_id']
#        print location['e'].keys()
        for edge in location['e'][OUTGOING_EDGE]:
            print edge[0]['u']
        
        exit()

if __name__ == '__main__':
#    writeCheckinSequenceGraphFile()
    createLocationFile()
