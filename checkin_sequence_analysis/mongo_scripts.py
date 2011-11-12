'''
Created on Nov 11, 2011

@author: kykamath
'''
import sys, time
sys.path.append('../')
from library.classes import GeneralMethods
from mongo_settings import checkinsCollection
from analysis.mr_analysis import filteredUserIterator
from settings import minLocationsTheUserHasCheckedin,\
    minUniqueUsersCheckedInTheLocation
    
userSet = set([userVector['user'] for userVector in filteredUserIterator(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation, fullRecord = True)])
for user in userSet:
    print user
    checkins = [(c['_id'], c['lid'], c['t'], time.mktime(c['t'].timetuple())) for c in checkinsCollection.find({'u': user})]
    for i in GeneralMethods.getElementsInWindow(checkins, 1):
        print i
    exit()
#for userVector in filteredUserIterator(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation, fullRecord = True):
#    print userVector