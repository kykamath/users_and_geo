'''
Created on Nov 11, 2011

@author: kykamath
'''
import sys
sys.path.append('../')
from mongo_settings import checkinsCollection
from analysis.mr_analysis import filteredUserIterator
from settings import minLocationsTheUserHasCheckedin,\
    minUniqueUsersCheckedInTheLocation
    
userSet = set([userVector['user'] for userVector in filteredUserIterator(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation, fullRecord = True)])
for user in userSet:
    print user
    for c in checkinsCollection.find({'u': user}): print (c['_id'], c['lid'], c['t'])
    exit()
#for userVector in filteredUserIterator(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation, fullRecord = True):
#    print userVector