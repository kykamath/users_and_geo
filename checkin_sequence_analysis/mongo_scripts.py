'''
Created on Nov 11, 2011

@author: kykamath
'''
import sys
from library.classes import GeneralMethods
sys.path.append('../')
from mongo_settings import checkinsCollection
from analysis.mr_analysis import filteredUserIterator
from settings import minLocationsTheUserHasCheckedin,\
    minUniqueUsersCheckedInTheLocation
    
userSet = set([userVector['user'] for userVector in filteredUserIterator(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation, fullRecord = True)])
for user in userSet:
    print user
    checkins = [(c['_id'], c['lid'], c['t'].epoch) for c in checkinsCollection.find({'u': user})]
    for i in GeneralMethods.getElementsInWindow(checkins, 2):
        print i
    exit()
#for userVector in filteredUserIterator(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation, fullRecord = True):
#    print userVector