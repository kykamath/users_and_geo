'''
Created on Oct 21, 2011

@author: kykamath
'''
import sys
from library.vector import Vector
from library.classes import GeneralMethods
from library.plotting import getDataDistribution, plotNorm
sys.path.append('../')
from library.clustering import KMeansClustering
from library.file_io import FileIO
from mongo_settings import venuesCollection
from analysis.mr_analysis import locationIterator,\
    filteredLocationToUserAndTimeMapIterator
from library.geo import isWithinBoundingBox, getLocationFromLid
from settings import brazos_valley_boundary, minUniqueUsersCheckedInTheLocation,\
    minLocationsTheUserHasCheckedin, placesLocationToUserMapFile,\
    placesClustersFile, placesImagesFolder
from collections import defaultdict
from itertools import groupby, combinations
from operator import itemgetter
import numpy as np
import matplotlib.pyplot as plt

def writeLocationToUserMap(place):
    name, boundary = place['name'], place['boundary']
    for location in filteredLocationToUserAndTimeMapIterator(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation):
        lid=getLocationFromLid(location['location'])
        if isWithinBoundingBox(lid, boundary): 
            title = venuesCollection.find_one({'lid':location['location']})
            if title: location['name'] = unicode(title['n']).encode("utf-8")
            else: location['name']=''
            for user in location['users'].keys()[:]: location['users'][str(user)]=location['users'][user]; del location['users'][user]
            FileIO.writeToFileAsJson(location, placesLocationToUserMapFile%name)
def locationToUserMapIterator(place): return FileIO.iterateJsonFromFile(placesLocationToUserMapFile%place['name'])
  
def writePlaceClusters(place):
    def meanClusteringDistance(userVectors, clusters):  
        clusterMeans = {}
        for clusterId in clusters: clusterMeans[clusterId] = Vector.getMeanVector(userVectors[user] for user in clusters[clusterId])
        return np.mean([Vector.euclideanDistance(clusterMeans[c1], clusterMeans[c2]) for c1, c2 in combinations(clusters,2)])
    GeneralMethods.runCommand('rm -rf %s'%placesClustersFile%place['name'])
    userVectors = defaultdict(dict)
    locationToUserMap = dict((l['location'], l) for l in locationToUserMapIterator(place))
    for lid in locationToUserMap:
        for user in locationToUserMap[lid]['users']: userVectors[user][lid]=sum(locationToUserMap[lid]['users'][user][d][db] for d in locationToUserMap[lid]['users'][user] for db in locationToUserMap[lid]['users'][user][d])
    for user in userVectors.keys()[:]: 
        if sum(userVectors[user].itervalues())<place['minTotalCheckins']: del userVectors[user]
    userVectorsToCluster = [(u, ' '.join([l.replace(' ', '_') for l in userVectors[u] for j in range(userVectors[u][l])])) for u in userVectors]
    resultsForVaryingK = []
    for k in range(3, 10):
        try:
            clusters = KMeansClustering(userVectorsToCluster, k).cluster()
            clusters = dict([(str(clusterId), [u for _,u  in users]) for clusterId, users in groupby(sorted(zip(clusters, userVectors), key=itemgetter(0)), key=itemgetter(0))])
            resultsForVaryingK.append((k, meanClusteringDistance(userVectors, clusters), clusters, dict((clusterId, GeneralMethods.getRandomColor()) for clusterId in clusters)))
        except Exception as e: print '*********** Exception while clustering k = %s'%k; pass
    FileIO.writeToFileAsJson(sorted(resultsForVaryingK, key=itemgetter(1))[-1], placesClustersFile%place['name'])
    for data in resultsForVaryingK: FileIO.writeToFileAsJson(data, placesClustersFile%place['name'])
def getBestClustering(place): 
    for data in FileIO.iterateJsonFromFile(placesClustersFile%place['name']): return data
def iteraterClusterings(place): 
    i = 0
    for data in FileIO.iterateJsonFromFile(placesClustersFile%place['name']): 
        if i!=0: yield data; 
        i+=1

def getPerLocationDistributionPlots(clustering, location, fileName):
    def getDayBlockMeansForClusters(users, userClusterMap):
        completeDayBlockDistribution = defaultdict(list)
        for user in users:
            if user in userClusterMap:
                dayBlockDistributionForUser = []
                for day in users[user]:
                    dayBlockDistributionForUser+=[int(dayBlock) for dayBlock in users[user][day] for i in range(users[user][day][dayBlock])]
                completeDayBlockDistribution[userClusterMap[user]]+=dayBlockDistributionForUser
        return [(k, np.mean(completeDayBlockDistribution[k]), np.std(completeDayBlockDistribution[k])) for k in completeDayBlockDistribution]
    def scale(val): return (val*4)+2
    def plotLocation(locationName, locationId, locationClustering, dayBlockMeans, dayBlockStandardDeviations, colorMap):
        classes, classDistribution = getDataDistribution(locationClustering.values())
        mu, sigma = dayBlockMeans, dayBlockStandardDeviations
        totalUsers = float(sum(classDistribution))
        for dist, mu, sigma, color in zip(classDistribution, mu, sigma, [colorMap[c] for c in classes]):
            if sigma==0: sigma=0.15
            plotNorm(dist/totalUsers, scale(mu), scale(sigma), color=color)
        plt.title('%s (%s)'%(locationName,locationId))
        plt.xlim(xmin=0,xmax=24)
#        plt.show()
        plt.savefig(fileName)
        plt.clf()
    userClusterMap = {}
    for clusterId, users in clustering[2].iteritems():
        for user in users: 
            if user in location['users']: userClusterMap[user]=clusterId
    means, deviations = zip(*getDayBlockMeansForClusters(location['users'], userClusterMap))[1:]
    plotLocation(location['name'], location['location'], userClusterMap, means, deviations, clustering[3])

def getLocationDistributionPlots(place):
    for clustering in iteraterClusterings(place):
        for location in locationToUserMapIterator(place): 
            print clustering[0], location['location']
            fileName=placesImagesFolder%place['name']+str(clustering[0])+'/'+ location['location'].replace(' ', '_').replace('.', '+')+'.png'
            FileIO.createDirectoryForFile(fileName)
            getPerLocationDistributionPlots(clustering, location, fileName)
    
place = {'name':'brazos', 'bondary':brazos_valley_boundary, 'minTotalCheckins':5}
#writeLocationToUserMap(place)
#writePlaceClusters(place)
getLocationDistributionPlots(place)
