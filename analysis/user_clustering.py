'''
Created on Oct 15, 2011

@author: kykamath
'''
import sys
from library.file_io import FileIO
sys.path.append('../')
from library.clustering import KMeansClustering
from analysis.mr_analysis import filteredUserIterator,\
    filteredLocationToUserAndTimeMapIterator, locationsForUsIterator
from settings import minLocationsTheUserHasCheckedin,\
    minUniqueUsersCheckedInTheLocation, locationClustersFile
from operator import itemgetter
from collections import defaultdict
from itertools import combinations
import numpy as np
from multiprocessing import Pool

def getUserVectors():
    ''' Returns a dict for user vectors across top 100 location dimensions.
    '''
    return dict((u['user'], dict(sorted(u['locations'].iteritems(), key=itemgetter(1), reverse=True)[:10000])) for u in filteredUserIterator(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation,  fullRecord = True))

def getDayBlockMeansForClusters(users, userClusterMap):
    completeDayBlockDistribution = defaultdict(list)
    for user in users:
        dayBlockDistributionForUser = []
        for day in users[user]:
            dayBlockDistributionForUser+=[int(dayBlock) for dayBlock in users[user][day] for i in range(users[user][day][dayBlock])]
        completeDayBlockDistribution[userClusterMap[user]]+=dayBlockDistributionForUser
    return [(k, np.mean(completeDayBlockDistribution[k]), np.std(completeDayBlockDistribution[k])) for k in completeDayBlockDistribution]

def getAverageDistanceBetweenClusters(meanDayblockValues): return np.mean([np.abs(m1-m2) for m1, m2 in combinations(meanDayblockValues,2)])
    

userVectors = getUserVectors()
locationsInUS = set(list(locationsForUsIterator(minUniqueUsersCheckedInTheLocation)))

def clusterLocation(location):
    dimensions = defaultdict(int)
    for u in location['users']:
        for lid in userVectors[u]: dimensions[lid]+=1
    dimensions = [d for d in dimensions if dimensions[d]>=2]
    userVectorsToCluster = [(u, ' '.join([l.replace(' ', '_') for l in userVectors[u] if l in dimensions for j in range(userVectors[u][l])])) for u in location['users']]
    resultsForVaryingK = []
    for k in range(2,6):
        try:
            cluster = KMeansClustering(userVectorsToCluster, k).cluster()
            userClusterMap = dict((k1,v) for k1,v in zip(location['users'], cluster))
            dayBlockMeansForClusters = getDayBlockMeansForClusters(location['users'], userClusterMap)
            userClusterMap = dict([(str(k2), v) for k2, v in userClusterMap.iteritems()])
            resultsForVaryingK.append([k, userClusterMap, zip(*dayBlockMeansForClusters)[1:], getAverageDistanceBetweenClusters(zip(*dayBlockMeansForClusters)[1])])
        except Exception as e: print '*********** Exception while clustering k = %s'%k; pass
    location['clustering'] = sorted(resultsForVaryingK, key=itemgetter(3))[-1]
    location['users'] = dict([(str(k),v) for k,v in location['users'].iteritems()])
    return location

def locationClusterIterator():
    for location in filter(lambda l: l['location'] in locationsInUS, filteredLocationToUserAndTimeMapIterator(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation)): yield location

def generateLocationClusterData():
#    p = Pool()
    totalLocations = len(list(locationClusterIterator()))
    i=1
    for location in locationClusterIterator():
        location = clusterLocation(location)
        print '%s of %s'%(i,totalLocations)
        FileIO.writeToFileAsJson(location, locationClustersFile)
        i+=1

if __name__ == '__main__':
    generateLocationClusterData()
    