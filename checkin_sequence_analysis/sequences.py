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
    minUniqueUsersCheckedInTheLocation, checkinSequenceGraphFile,\
    checkinSequenceLocationRegexFolder
from operator import itemgetter

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

def createLocationFile(regex):
    def getCheckinsForUser(user): return [[checkin['_id'], checkin['lid'], time.mktime(checkin['t'].timetuple())] for checkin in checkinsCollection.find({'u':user})]
    fileName = checkinSequenceLocationRegexFolder+regex
    for location in checkinSequenceLocationsCollection.find({'n':{'$regex':regex}}):
        userCheckins = {}
        users = set([edge[0]['u'] for type in [OUTGOING_EDGE, INCOMING_EDGE] for edge in location['e'][type]])
        for user in users: 
            if user not in userCheckins: userCheckins[str(user)] = getCheckinsForUser(user)
        location['users'] = userCheckins
        location['edges'] = location['e']; del location['e']
        location['checkins'] = location['c']; del location['c']
        location['lid'] = location['_id']; del location['_id']
        location['name'] = location['n']; del location['n']
        FileIO.writeToFileAsJson(location, fileName)

class UserVectorSelection:
    @staticmethod
    def latestNCheckins(checkinId, user, users, n=1, **kwargs):
        print checkinId, users[user]
class NeighboringClusters():
    @staticmethod
    def getLocationClustersFromCheckins(checkins, users, userVectorSelectionMethod):
        for checkin in checkins:
            userCheckins = users[str(checkin['u'])]
            assert userCheckins == sorted(userCheckins, key=itemgetter(0), reverse=True)
            print checkin, len(userCheckins)
            exit()
        exit()
#        print len(checkins), users.keys()
    @staticmethod
    def getNeigboringLocationClusters(regex, edgeType = INCOMING_EDGE):
        inputFileName = checkinSequenceLocationRegexFolder+regex
        checkinSelectionIndex = {INCOMING_EDGE:0, OUTGOING_EDGE:1}[edgeType]
        for data in FileIO.iterateJsonFromFile(inputFileName):
            data['users']
            if edgeType==INCOMING_EDGE: checkins = [edge[checkinSelectionIndex] for edge in data['edges'][edgeType] if edge[checkinSelectionIndex]['lid']!=data['lid']]
            NeighboringClusters.getLocationClustersFromCheckins(checkins, data['users'], UserVectorSelection.latestNCheckins)
    #        for c in checkins:
    #            print data['lid'], c
    #        print type(data['edges'][checkinsType]), 
            exit()

if __name__ == '__main__':
#    writeCheckinSequenceGraphFile()
#    createLocationFile(regex='cafe')
    NeighboringClusters.getNeigboringLocationClusters('cafe')
