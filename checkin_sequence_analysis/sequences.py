'''
Created on Nov 11, 2011

@author: kykamath
'''
import sys, time
sys.path.append('../')
from library.classes import GeneralMethods
from library.file_io import FileIO
from mongo_settings import checkinsCollection
from analysis.mr_analysis import filteredUserIterator
from settings import minLocationsTheUserHasCheckedin,\
    minUniqueUsersCheckedInTheLocation, checkinSequenceGraphFile

def writeCheckinSequenceGraphFile():   
    userSet = set([userVector['user'] for userVector in filteredUserIterator(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation, fullRecord = True)])
    count, total = 1, len(userSet)
    for user in userSet:
        print user, count, total
        checkins = [(c['_id'], c['lid'], time.mktime(c['t'].timetuple())) for c in checkinsCollection.find({'u': user})]
        for i in GeneralMethods.getElementsInWindow(checkins, 2): FileIO.writeToFileAsJson([user, i], checkinSequenceGraphFile)
        count+=1
#for userVector in filteredUserIterator(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation, fullRecord = True):
#    print userVector

writeCheckinSequenceGraphFile()