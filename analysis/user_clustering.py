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
    return [(k, np.mean(completeDayBlockDistribution[k])) for k in completeDayBlockDistribution]

def getAverageDistanceBetweenClusters(meanDayblockValues): return np.mean([np.abs(m1-m2) for m1, m2 in combinations(meanDayblockValues,2)])
    

userVectors = getUserVectors()
locationsInUS = set(list(locationsForUsIterator(minUniqueUsersCheckedInTheLocation)))
i = 0
clusters = []
for location in filter(lambda l: l['location'] in locationsInUS, filteredLocationToUserAndTimeMapIterator(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation)): 
#    userClusterMap = dict((u, random.randint(0,2)) for u in location['users'])
#    dayBlockMeansForClusters = getDayBlockMeansForClusters(location['users'], userClusterMap)
#    print zip(*dayBlockMeansForClusters)[1], getAverageDistanceBetweenClusters(zip(*dayBlockMeansForClusters)[1])

#    userVectorsToCluster = [(u, ' '.join([l.replace(' ', '_') for l in userVectors[u] for j in range(userVectors[u][l])])) for u in location['users']]
#    userVectorsToCluster = [(u, ' '.join([l.replace(' ', '_') for l in userVectors[u] for j in range(1)])) for u in location['users']]
#    k = 2
#    clusters.append(KMeansClustering(userVectorsToCluster, k).cluster())
#
#    i+=1
#    if i==100:
#        for i in clusters:
#            print i
#        exit()

    dimensions = defaultdict(int)
    for u in location['users']:
        for lid in userVectors[u]: dimensions[lid]+=1
    dimensions = [d for d in dimensions if dimensions[d]>=2]
    print dimensions
    
#    exit()

#    dimensions = sorted(list(set([lid for u in location['users'] for lid in userVectors[u] ])))
    documents = []
    for user in location['users']:
        userDocument = [0.0 if lid not in userVectors[user] else float(userVectors[user][lid]) for lid in dimensions]
#        userDocument = []
#        print location['users'][user]
#        for lid in dimensions:
#            if lid not in location['users'][user]: userDocument.append(0.0)
#            else:  userDocument.append(float(location['users'][user][lid]))
        documents.append(userDocument)
#        exit()
#    for d in documents:
#        print d
    documents = np.array(documents)
#    print documents
    whitened = whiten(documents)
    book = np.array((whitened[0],whitened[2]))

#    whitened = whiten(documents)
#    codes = 2
    userClusterMap = dict((k,v) for k,v in zip(location['users'], list(kmeans2(whitened,book)[1])))
#    userClusterMap = dict((k,v) for k,v in zip(location['users'], list(kmeans2(whitened,codes)[1])))
    print userClusterMap
    dayBlockMeansForClusters = getDayBlockMeansForClusters(location['users'], userClusterMap)
    print zip(*dayBlockMeansForClusters)[1], getAverageDistanceBetweenClusters(zip(*dayBlockMeansForClusters)[1])
    exit()