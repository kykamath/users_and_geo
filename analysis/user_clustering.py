'''
Created on Oct 15, 2011

@author: kykamath
'''
import sys
sys.path.append('../')
from library.clustering import KMeansClustering, EMTextClustering
from analysis.mr_analysis import filteredUserIterator,\
    filteredLocationToUserAndTimeMapIterator, locationsForUsIterator
from settings import minLocationsTheUserHasCheckedin,\
    minUniqueUsersCheckedInTheLocation
from operator import itemgetter
from collections import defaultdict
from itertools import combinations
import numpy as np
import random
from scipy.cluster.vq import vq, kmeans, whiten, kmeans2

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
        cluster = KMeansClustering(userVectorsToCluster, k).cluster()
        userClusterMap = dict((k1,v) for k1,v in zip(location['users'], cluster))
        dayBlockMeansForClusters = getDayBlockMeansForClusters(location['users'], userClusterMap)
        resultsForVaryingK.append((k, userClusterMap, zip(*dayBlockMeansForClusters)[1:], getAverageDistanceBetweenClusters(zip(*dayBlockMeansForClusters)[1])))
    return sorted(resultsForVaryingK, key=itemgetter(3))[-1]

for location in filter(lambda l: l['location'] in locationsInUS, filteredLocationToUserAndTimeMapIterator(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation)): 
    print clusterLocation(location)
#    dimensions = defaultdict(int)
#    for u in location['users']:
#        for lid in userVectors[u]: dimensions[lid]+=1
#    dimensions = [d for d in dimensions if dimensions[d]>=2]
#
#    userVectorsToCluster = [(u, ' '.join([l.replace(' ', '_') for l in userVectors[u] if l in dimensions for j in range(userVectors[u][l])])) for u in location['users']]
#    resultsForVaryingK = []
#    for k in range(2,6):
#        cluster = KMeansClustering(userVectorsToCluster, k).cluster()
#        userClusterMap = dict((k1,v) for k1,v in zip(location['users'], cluster))
#        dayBlockMeansForClusters = getDayBlockMeansForClusters(location['users'], userClusterMap)
#        resultsForVaryingK.append((k, userClusterMap, zip(*dayBlockMeansForClusters)[1:], getAverageDistanceBetweenClusters(zip(*dayBlockMeansForClusters)[1])))
#    
#    selectedClustering = sorted(resultsForVaryingK, key=itemgetter(3))[-1]
#    for k in resultsForVaryingK: print k
#    print selectedClustering
    
#    i+=1
#    if i==100:
#        for i in clusters:
#            print i
#        exit()

    
#    documents = []
#    for user in location['users']:
#        userDocument = [0.0 if lid not in userVectors[user] else float(userVectors[user][lid]) for lid in dimensions]
#        documents.append(userDocument)
#    documents = np.array(documents)
#    whitened = whiten(documents)
#    book = np.array((whitened[0],whitened[2]))
#
##    whitened = whiten(documents)
##    codes = 2
#    userClusterMap = dict((k,v) for k,v in zip(location['users'], cluster))
##    userClusterMap = dict((k,v) for k,v in zip(location['users'], list(kmeans2(whitened,codes)[1])))
#    print userClusterMap
#    dayBlockMeansForClusters = getDayBlockMeansForClusters(location['users'], userClusterMap)
#    print zip(*dayBlockMeansForClusters)[1], getAverageDistanceBetweenClusters(zip(*dayBlockMeansForClusters)[1])
    exit()