'''
What kind of people visit chipotle at different times?
What kind of people are at chipotle right now?
What kind of people will be at chipotle 15 days from now? Make this decision under un certainity.
Created on Oct 15, 2011

@author: kykamath
'''
import sys
sys.path.append('../')
from library.file_io import FileIO
from mongo_settings import venuesCollection
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
from library.plotting import plotNorm, getDataDistribution
import matplotlib.pyplot as plt

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
    if resultsForVaryingK: location['clustering'] = sorted(resultsForVaryingK, key=itemgetter(3))[-1]
    location['users'] = dict([(str(k),v) for k,v in location['users'].iteritems()])
    return location

def locationClusterIterator():
    for location in filter(lambda l: l['location'] in locationsInUS, filteredLocationToUserAndTimeMapIterator(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation)): yield location

def generateLocationClusterData():
#    p = Pool()
#    totalLocations = len(list(locationClusterIterator()))
#    i=1
    for location in locationClusterIterator():
        location = clusterLocation(location)
#        print '%s of %s'%(i,totalLocations)
        FileIO.writeToFileAsJson(location, locationClustersFile)
#        i+=1
#    p = Pool()
#    for location in p.imap(clusterLocation, locationClusterIterator()): FileIO.writeToFileAsJson(location, locationClustersFile)

def plotLocationDistribution():
    '''Types of locations seen: 
        => Locations where different people have to be at same time: Example office, pub
        => Locations that different people choose to go at different times: cafe+party place
       Big cluster suggests most people who come to a location go to similar locations (implies similar people). 
        Their mean suggests the most poplar time to go to that location.
    '''
    def scale(val): return (val*4)+2#val*2*4+2
    for location in FileIO.iterateJsonFromFile(locationClustersFile):
        if 'clustering' in location:
            classes, classDistribution = getDataDistribution(location['clustering'][1].values())
            mu, sigma = location['clustering'][2][0], location['clustering'][2][1]
            totalUsers = float(sum(classDistribution))
            for dist, mu, sigma in zip(classDistribution, mu, sigma):
                if sigma==0: sigma=0.15
                print dist/totalUsers
                plotNorm(dist/totalUsers, scale(mu), scale(sigma))
            title = venuesCollection.find_one({'lid':location['location']})
            if title!=None: title = unicode(title['n']).encode("utf-8")
            else: title = ''
            plt.title('%s (%s)'%(title,location['location']))
            plt.xlim(xmin=0,xmax=24)
            plt.show()


if __name__ == '__main__':
#    generateLocationClusterData()
    plotLocationDistribution()
    