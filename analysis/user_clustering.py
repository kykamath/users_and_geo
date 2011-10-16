'''
Created on Oct 15, 2011

@author: kykamath
'''
import sys
from library.clustering import KMeansClustering
sys.path.append('../')
from analysis.mr_analysis import filteredUserIterator,\
    filteredLocationToUserAndTimeMapIterator, locationsForUsIterator
from settings import minLocationsTheUserHasCheckedin,\
    minUniqueUsersCheckedInTheLocation
from operator import itemgetter
from collections import defaultdict
from itertools import combinations
import numpy as np
import random

def getUserVectors():
    ''' Returns a dict for user vectors across top 100 location dimensions.
    '''
    return dict((u['user'], dict(sorted(u['locations'].iteritems(), key=itemgetter(1), reverse=True)[:100])) for u in filteredUserIterator(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation,  fullRecord = True))

def getDayBlockMeansForClusters(users, userClusterMap):
    completeDayBlockDistribution = defaultdict(list)
    for user in users:
        dayBlockDistributionForUser = []
        for day in users[user]:
            dayBlockDistributionForUser+=[int(dayBlock) for dayBlock in users[user][day] for i in range(users[user][day][dayBlock])]
        completeDayBlockDistribution[userClusterMap[user]]+=dayBlockDistributionForUser
    return [(k, np.mean(completeDayBlockDistribution[k])) for k in completeDayBlockDistribution]

def getAverageDistanceBetweenClusters(meanDayblockValues): return np.mean([np.abs(m1-m2) for m1, m2 in combinations(meanDayblockValues,2)])
    

userVectors = getUserVectors()
locationsInUS = set(list(locationsForUsIterator(minUniqueUsersCheckedInTheLocation)))

for location in filter(lambda l: l['location'] in locationsInUS, filteredLocationToUserAndTimeMapIterator(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation)): 
#    userClusterMap = dict((u, random.randint(0,2)) for u in location['users'])
#    dayBlockMeansForClusters = getDayBlockMeansForClusters(location['users'], userClusterMap)
#    print zip(*dayBlockMeansForClusters)[1], getAverageDistanceBetweenClusters(zip(*dayBlockMeansForClusters)[1])

#    userVectorsToCluster = [(u, ' '.join([i.replace(' ', '_') for i in userVectors[u].keys()]))for u in location['users']]
#    k = 2
#    clusters = KMeansClustering(userVectorsToCluster, k).cluster()
#    print clusters
    for u in location['users']:
        print u, userVectors[u]
#    for k in [(u, ' '.join([i.replace(' ', '_') for i in userVectors[u].keys() for j in range()]))for u in location['users']]:
#        print k
    exit()
