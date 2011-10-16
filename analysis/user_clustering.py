'''
Created on Oct 15, 2011

@author: kykamath
'''
import sys
sys.path.append('../')
from analysis.mr_analysis import filteredUserIterator,\
    filteredLocationToUserAndTimeMapIterator, locationsForUsIterator
from settings import minLocationsTheUserHasCheckedin,\
    minUniqueUsersCheckedInTheLocation
from operator import itemgetter
from collections import defaultdict
import numpy as np
import random

def getUserVectors():
    ''' Returns a dict for user vectors across top 100 location dimensions.
    '''
    return dict((u['user'], dict(sorted(u['locations'].iteritems(), key=itemgetter(1), reverse=True)[:100])) for u in filteredUserIterator(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation,  fullRecord = True))

#userVectors = getUserVectors()
def getDayBlockDistributionForUsers(users, userClusterMap):
    completeDayBlockDistribution = defaultdict(list)
    for user in users:
        dayBlockDistributionForUser = []
        for day in users[user]:
            dayBlockDistributionForUser+=[int(dayBlock) for dayBlock in users[user][day] for i in range(users[user][day][dayBlock])]
        completeDayBlockDistribution[userClusterMap[user]]+=dayBlockDistributionForUser
    print [(k, np.mean(completeDayBlockDistribution[k])) for k in completeDayBlockDistribution]
    
locationsInUS = set(list(locationsForUsIterator(minUniqueUsersCheckedInTheLocation)))
for location in filter(lambda l: l['location'] in locationsInUS, filteredLocationToUserAndTimeMapIterator(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation)): 
    userClusterMap = dict((u, random.randint(0,2)) for u in location['users'])
    getDayBlockDistributionForUsers(location['users'], userClusterMap)
    
