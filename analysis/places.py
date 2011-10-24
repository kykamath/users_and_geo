'''
model iwth probabilites
Created on Oct 21, 2011

@author: kykamath
'''
import sys, datetime
sys.path.append('../')
from library.vector import Vector
from library.classes import GeneralMethods
from library.plotting import getDataDistribution, plotNorm
from analysis import SpotsKML
from library.clustering import KMeansClustering
from library.plotting import getDataDistribution
from library.file_io import FileIO
from mongo_settings import venuesCollection
from analysis.mr_analysis import locationIterator,\
    filteredLocationToUserAndTimeMapIterator
from library.geo import isWithinBoundingBox, getLocationFromLid,\
    getLidFromLocation
from settings import brazos_valley_boundary, minUniqueUsersCheckedInTheLocation,\
    minLocationsTheUserHasCheckedin, placesLocationToUserMapFile,\
    placesImagesFolder, locationToUserAndExactTimeMapFile,\
    austin_tx_boundary, placesKMLsFolder, placesAnalysisFolder,\
    placesLocationWithClusterInfoFile, placesUserClustersFile,\
    placesLocationClustersFile
from collections import defaultdict
from itertools import groupby, combinations
from operator import itemgetter
import numpy as np
import matplotlib.pyplot as plt

def meanClusteringDistance(clusterMeans): return np.mean([Vector.euclideanDistance(Vector(dict(c1)), Vector(dict(c2))) for c1, c2 in combinations(clusterMeans,2)])

def writeLocationToUserMap(place):
    name, boundary = place['name'], place['boundary']
    for location in filteredLocationToUserAndTimeMapIterator(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation, inputFile=locationToUserAndExactTimeMapFile):
        lid=getLocationFromLid(location['location'])
        if isWithinBoundingBox(lid, boundary): 
            title = venuesCollection.find_one({'lid':location['location']})
            if title: location['name'] = unicode(title['n']).encode("utf-8")
            else: location['name']=''
            for user in location['users'].keys()[:]: location['users'][str(user)]=location['users'][user]; del location['users'][user]
            location['noOfCheckins']=sum([len(epochs) for user, userVector in location['users'].iteritems() for day, dayVector in userVector.iteritems() for db, epochs in dayVector.iteritems()])
            FileIO.writeToFileAsJson(location, placesLocationToUserMapFile%name)
def locationToUserMapIterator(place, minCheckins=0): 
    for location in FileIO.iterateJsonFromFile(placesLocationToUserMapFile%place['name']):
        if location['noOfCheckins']>=minCheckins: yield location
  
def writeUserClusters(place):
    numberOfTopFeatures = 5
    GeneralMethods.runCommand('rm -rf %s'%placesUserClustersFile%place['name'])
    userVectors = defaultdict(dict)
    locationToUserMap = dict((l['location'], l) for l in locationToUserMapIterator(place))
    for lid in locationToUserMap:
        for user in locationToUserMap[lid]['users']: 
            userVectors[user][lid.replace(' ', '_')]=sum(len(locationToUserMap[lid]['users'][user][d][db]) for d in locationToUserMap[lid]['users'][user] for db in locationToUserMap[lid]['users'][user][d])
    for user in userVectors.keys()[:]: 
        if sum(userVectors[user].itervalues())<place['minTotalCheckins']: del userVectors[user]
    resultsForVaryingK = []
    for k in range(5,20):
        try:
            clusters = KMeansClustering(userVectors.iteritems(), k, documentsAsDict=True).cluster(normalise=True, assignAndReturnDetails=True, repeats=5, numberOfTopFeatures=numberOfTopFeatures)
            for clusterId, features in clusters['bestFeatures'].items()[:]: clusters['bestFeatures'][str(clusterId)]=[(lid.replace('_', ' '), score)for lid, score in features]; del clusters['bestFeatures'][clusterId]
            for clusterId, users in clusters['clusters'].items()[:]: clusters['clusters'][str(clusterId)]=users; del clusters['clusters'][clusterId]
            resultsForVaryingK.append((k, meanClusteringDistance(clusters['bestFeatures'].itervalues()), clusters, dict((clusterId, GeneralMethods.getRandomColor()) for clusterId in clusters['clusters'])))
        except Exception as e: print '*********** Exception while clustering k = %s'%k; pass
    FileIO.writeToFileAsJson(sorted(resultsForVaryingK, key=itemgetter(1))[0], placesUserClustersFile%place['name'])
    for data in resultsForVaryingK: FileIO.writeToFileAsJson(data, placesUserClustersFile%place['name'])
def getBestClustering(place, idOnly=False): 
    for data in FileIO.iterateJsonFromFile(placesUserClustersFile%place['name']): 
        if idOnly: return data[0]
        return data
def iteraterClusterings(place): 
    i = 0
    for data in FileIO.iterateJsonFromFile(placesUserClustersFile%place['name']): 
        if i!=0: yield data; 
        i+=1

def writeLocationsWithClusterInfoFile(place):
    GeneralMethods.runCommand('rm -rf %s'%placesLocationWithClusterInfoFile%place['name'])
    for clustering in iteraterClusterings(place):
        dataToWrite, userClusterMap = {}, {}
        for clusterId, users in clustering[2]['clusters'].iteritems(): 
            for user in users: userClusterMap[user]=clusterId
        locationMap = defaultdict(dict)
        for location in locationToUserMapIterator(place):
            locationMap[location['location']] = {'name':unicode(location['name']).encode("utf-8"), 'checkins':defaultdict(list)}
            for user, userVector in location['users'].iteritems():
                if user in userClusterMap:
                    for day, dayVector in userVector.iteritems():
                        for db, epochs in dayVector.iteritems():
                            locationMap[location['location']]['checkins'][userClusterMap[user]]+=epochs
            dataToWrite[str(clustering[0])]=locationMap
        FileIO.writeToFileAsJson(dataToWrite,placesLocationWithClusterInfoFile%place['name']) 
def getLocationWithClusterDetails(place, clusterId):
    for data in FileIO.iterateJsonFromFile(placesLocationWithClusterInfoFile%place['name']):
        if str(clusterId) in data: return data
        
def writeLocationClusters(place):
    GeneralMethods.runCommand('rm -rf %s'%placesLocationClustersFile%place['name'])
    clusterId = getBestClustering(place, idOnly=True)
    locations = getLocationWithClusterDetails(place, clusterId)
    locationVectorsToCluster = [(location, dict((clusterId, len(epochs)) for clusterId, epochs in checkins['checkins'].iteritems())) for location, checkins in locations.values()[0].iteritems()]
    resultsForVaryingK = []
    for k in range(7,20):
        try:
            clusters = KMeansClustering(locationVectorsToCluster, k, documentsAsDict=True).cluster(normalise=True, assignAndReturnDetails=True, repeats=5)
            for clusterId, features in clusters['bestFeatures'].items()[:]: clusters['bestFeatures'][str(clusterId)]=[(lid.replace('_', ' '), score)for lid, score in features]; del clusters['bestFeatures'][clusterId]
            for clusterId, users in clusters['clusters'].items()[:]: clusters['clusters'][str(clusterId)]=users; del clusters['clusters'][clusterId]
            resultsForVaryingK.append((k, meanClusteringDistance(clusters['bestFeatures'].itervalues()), clusters, dict((clusterId, GeneralMethods.getRandomColor()) for clusterId in clusters['clusters'])))
        except Exception as e: print '*********** Exception while clustering k = %s'%k; pass
    FileIO.writeToFileAsJson(sorted(resultsForVaryingK, key=itemgetter(1))[0], placesLocationClustersFile%place['name'])
    for data in resultsForVaryingK: FileIO.writeToFileAsJson(data, placesLocationClustersFile%place['name'])


def getLocationsCheckinDistribution(place):
    checkinDistribution = {}
    for location in locationToUserMapIterator(place):
        checkinDistribution[location['location']]=sum([len(epochs) for user, userVector in location['users'].iteritems() for day, dayVector in userVector.iteritems() for db, epochs in dayVector.iteritems()])
    dataX, dataY = getDataDistribution(checkinDistribution.values())
    plt.loglog(dataX,dataY)
    outputFile = placesAnalysisFolder%place['name']+'locationsCheckinDistribution.png'
    FileIO.createDirectoryForFile(outputFile)
    plt.savefig(outputFile)
    
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

def getLocationScatterPlots(place):
    def scatterPlot(clustering, location, fileName):
        userClusterMap = {}
        for clusterId, users in clustering[2].iteritems():
            for user in users: 
                if user in location['users']: userClusterMap[user]=clusterId
        scatterData = defaultdict(dict)
        clusterMap = clustering[3]
        for user, userVector in location['users'].iteritems():
            if user in userClusterMap:
                for d in userVector:
                    for db in userVector[d]:
                        for h in [datetime.datetime.fromtimestamp(ep).hour for ep in userVector[d][db]]:
                            if h not in scatterData[userClusterMap[user]]: scatterData[userClusterMap[user]][h]=0
                            scatterData[userClusterMap[user]][h]+=1
        for cluster, clusterInfo in scatterData.iteritems(): plt.scatter(clusterInfo.keys(), clusterInfo.values(), color=clusterMap[cluster], label=cluster)
        plt.title('%s (%s)'%(location['name'],location['location'])),plt.legend()
#        plt.show()
        plt.xlim(xmin=0,xmax=24)
        plt.savefig(fileName), plt.clf()
    for clustering in iteraterClusterings(place):
        for location in locationToUserMapIterator(place): 
            print clustering[0], location['location']
            fileName=placesImagesFolder%place['name']+str(clustering[0])+'/'+ location['location'].replace(' ', '_').replace('.', '+')+'.png'
            FileIO.createDirectoryForFile(fileName)
            scatterPlot(clustering, location, fileName)

def getClusterKMLs(place):
    userVectors = defaultdict(dict)
    locationToUserMap = dict((l['location'], l) for l in locationToUserMapIterator(place))
    for lid in locationToUserMap:
        for user in locationToUserMap[lid]['users']: userVectors[user][lid]=sum(len(locationToUserMap[lid]['users'][user][d][db]) for d in locationToUserMap[lid]['users'][user] for db in locationToUserMap[lid]['users'][user][d])
    for user in userVectors.keys()[:]: 
        if sum(userVectors[user].itervalues())<place['minTotalCheckins']: del userVectors[user]
    print userVectors
    for clustering in iteraterClusterings(place):
        locationDistributionForClusters = defaultdict(dict)
        for clusterId, users in clustering[2].iteritems():
            for user in users:
                for lid, count in userVectors[user].iteritems():
                    if lid not in locationDistributionForClusters[clusterId]: locationDistributionForClusters[clusterId][lid]=0
                    locationDistributionForClusters[clusterId][lid]+=count
        colorMap = clustering[3]
        for clusterId, locationDistributionForClusters in locationDistributionForClusters.iteritems(): 
            kml = SpotsKML()
            kml.addLocationPointsWithTitles([(getLocationFromLid(p[0]), locationToUserMap[p[0]]['name']) for p in  sorted(locationDistributionForClusters.iteritems(), key=itemgetter(1), reverse=True)[:5]], color=colorMap[clusterId])
            outputKMLFile=placesKMLsFolder%place['name']+'locations/%s/%s.kml'%(str(clustering[0]), str(clusterId))
            FileIO.createDirectoryForFile(outputKMLFile)
            kml.write(outputKMLFile)
            
    
def getUserClusterDetails(place):
    clusters = getBestClustering(place)
    locationNameMap = dict((location['location'], location['name'])for location in locationToUserMapIterator(place))
    for clusterId, features in  sorted(clusters[2]['bestFeatures'].iteritems(), key=lambda k: int(k[0])):
        print clusterId, [locationNameMap[lid] for lid, _ in features]
    
    
place = {'name':'brazos', 'boundary':brazos_valley_boundary, 'minTotalCheckins':5}
#place = {'name':'austin_tx', 'boundary':austin_tx_boundary, 'minTotalCheckins':5}

#writeLocationToUserMap(place)
writeUserClusters(place)
writeLocationsWithClusterInfoFile(place)
#writeLocationClusters(place)

getUserClusterDetails(place)

#print len(list(locationToUserMapIterator(place)))
#print len(list(locationToUserMapIterator(place,minCheckins=100)))

#getLocationsCheckinDistribution(place)

#getLocationDistributionPlots(place)
#getLocationScatterPlots(place)
#getClusterKMLs(place)