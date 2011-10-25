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
from library.stats import getWeitzmanOVL
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
    GeneralMethods.runCommand('rm -rf %s'%placesLocationToUserMapFile%name)
    for location in filteredLocationToUserAndTimeMapIterator(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation, inputFile=locationToUserAndExactTimeMapFile):
        lid=getLocationFromLid(location['location'])
        if isWithinBoundingBox(lid, boundary): 
            title = venuesCollection.find_one({'lid':location['location']})
            if title: location['name'] = unicode(title['n']).encode("utf-8")
            else: location['name']=''
            for user in location['users'].keys()[:]: location['users'][str(user)]=location['users'][user]; del location['users'][user]
            location['noOfCheckins']=sum([len(epochs) for user, userVector in location['users'].iteritems() for day, dayVector in userVector.iteritems() for db, epochs in dayVector.iteritems()])
            if location['noOfCheckins']>place['minLocationCheckins']: FileIO.writeToFileAsJson(location, placesLocationToUserMapFile%name)
def locationToUserMapIterator(place, minCheckins=0, maxCheckins=()): 
    for location in FileIO.iterateJsonFromFile(placesLocationToUserMapFile%place['name']):
        if location['noOfCheckins']<maxCheckins and location['noOfCheckins']>=minCheckins: yield location
  
def writeUserClusters(place):
    numberOfTopFeatures = 10000
    GeneralMethods.runCommand('rm -rf %s'%placesUserClustersFile%place['name'])
    userVectors = defaultdict(dict)
    locationToUserMap = dict((l['location'], l) for l in locationToUserMapIterator(place, minCheckins=50))
    for lid in locationToUserMap:
        for user in locationToUserMap[lid]['users']: 
            userVectors[user][lid.replace(' ', '_')]=sum(len(locationToUserMap[lid]['users'][user][d][db]) for d in locationToUserMap[lid]['users'][user] for db in locationToUserMap[lid]['users'][user][d])
    for user in userVectors.keys()[:]: 
        if sum(userVectors[user].itervalues())<place['minUserCheckins']: del userVectors[user]
    resultsForVaryingK = []
    for k in range(60,100):
        try:
            print 'Clustering with k=%s'%k
            clusters = KMeansClustering(userVectors.iteritems(), k, documentsAsDict=True).cluster(normalise=True, assignAndReturnDetails=True, repeats=5, numberOfTopFeatures=numberOfTopFeatures, algorithmSource='biopython')
            error=clusters['error']
            for clusterId, features in clusters['bestFeatures'].items()[:]: clusters['bestFeatures'][str(clusterId)]=[(lid.replace('_', ' '), score)for lid, score in features]; del clusters['bestFeatures'][clusterId]
            for clusterId, users in clusters['clusters'].items()[:]: clusters['clusters'][str(clusterId)]=users; del clusters['clusters'][clusterId]
            if error: resultsForVaryingK.append((k, error, clusters, dict((clusterId, GeneralMethods.getRandomColor()) for clusterId in clusters['clusters'])))
            else: resultsForVaryingK.append((k, meanClusteringDistance(clusters['bestFeatures'].itervalues()), clusters, dict((clusterId, GeneralMethods.getRandomColor()) for clusterId in clusters['clusters'])))
        except Exception as e: print '*********** Exception while clustering k = %s; %s'%(k, e); pass
    FileIO.writeToFileAsJson(min(resultsForVaryingK, key=itemgetter(1)), placesUserClustersFile%place['name'])
    for data in resultsForVaryingK: FileIO.writeToFileAsJson(data, placesUserClustersFile%place['name'])
def getBestUserClustering(place, idOnly=False): 
    for data in FileIO.iterateJsonFromFile(placesUserClustersFile%place['name']): 
        if idOnly: return data[0]
        return data
def iteraterUserClusterings(place): 
    i = 0
    for data in FileIO.iterateJsonFromFile(placesUserClustersFile%place['name']): 
        if i!=0: yield data; 
        i+=1
def getUserClusteringDetails(place, clustering):
    locationNameMap = dict((location['location'], location['name'])for location in locationToUserMapIterator(place))
    clusterDetails = defaultdict(list)
    for (clusterId, users), (clusterId, features) in zip(clustering[2]['clusters'].iteritems(), clustering[2]['bestFeatures'].iteritems()):
        if len(users)>place.get('minimunUsersInUserCluster', 0): clusterDetails[clusterId] = {'users': users, 'locations': [(lid, locationNameMap[lid], score) for lid, score in features]}
    return clusterDetails

def writeLocationsWithClusterInfoFile(place):
    GeneralMethods.runCommand('rm -rf %s'%placesLocationWithClusterInfoFile%place['name'])
    for clustering in iteraterUserClusterings(place):
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
    clusterId = getBestUserClustering(place, idOnly=True)
    locations = getLocationWithClusterDetails(place, clusterId)
    locationVectorsToCluster = [(location, dict((clusterId, len(epochs)) for clusterId, epochs in checkins['checkins'].iteritems())) for location, checkins in locations.values()[0].iteritems()]
    resultsForVaryingK = []
    for k in range(60,80):
        try:
            print 'Clustering with k=%s'%k
            clusters = KMeansClustering(locationVectorsToCluster, k, documentsAsDict=True).cluster(normalise=True, assignAndReturnDetails=True, repeats=5, algorithmSource='biopython')
            error=clusters['error']
            for clusterId, features in clusters['bestFeatures'].items()[:]: clusters['bestFeatures'][str(clusterId)]=[(lid.replace('_', ' '), score)for lid, score in features]; del clusters['bestFeatures'][clusterId]
            for clusterId, users in clusters['clusters'].items()[:]: clusters['clusters'][str(clusterId)]=users; del clusters['clusters'][clusterId]
            if error: resultsForVaryingK.append((k, error, clusters, dict((clusterId, GeneralMethods.getRandomColor()) for clusterId in clusters['clusters'])))
            else: resultsForVaryingK.append((k, meanClusteringDistance(clusters['bestFeatures'].itervalues()), clusters, dict((clusterId, GeneralMethods.getRandomColor()) for clusterId in clusters['clusters'])))
#            resultsForVaryingK.append((k, meanClusteringDistance(clusters['bestFeatures'].itervalues()), clusters, dict((clusterId, GeneralMethods.getRandomColor()) for clusterId in clusters['clusters'])))
        except Exception as e: print '*********** Exception while clustering k = %s; %s'%(k, e); pass
    FileIO.writeToFileAsJson(min(resultsForVaryingK, key=itemgetter(1)), placesLocationClustersFile%place['name'])
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
    for clustering in iteraterUserClusterings(place):
        for location in locationToUserMapIterator(place): 
            print clustering[0], location['location']
            fileName=placesImagesFolder%place['name']+str(clustering[0])+'/'+ location['location'].replace(' ', '_').replace('.', '+')+'.png'
            FileIO.createDirectoryForFile(fileName)
            getPerLocationDistributionPlots(clustering, location, fileName)

def getLocationPlots(place, type='scatter'):
    clustering = getBestUserClustering(place)
    validClusters = getUserClusteringDetails(place, clustering).keys()
    def scatterPlot(clustering, location, fileName):
        userClusterMap = {}
        for clusterId, users in clustering[2]['clusters'].iteritems():
            for user in users: 
                if user in location['users']: userClusterMap[user]=clusterId
        scatterData = defaultdict(dict)
        clusterMap = clustering[3]
        for user, userVector in location['users'].iteritems():
            if user in userClusterMap:
                for d in userVector:
                    for db in userVector[d]:
                        for h in [(datetime.datetime.fromtimestamp(ep).hour-6)%24 for ep in userVector[d][db]]:
                            if h not in scatterData[userClusterMap[user]]: scatterData[userClusterMap[user]][h]=0
                            scatterData[userClusterMap[user]][h]+=1
#        total = float(sum([k for cluster, clusterInfo in scatterData.iteritems() for k, v in clusterInfo.iteritems() for i in range(v)]))
        for cluster, clusterInfo in scatterData.iteritems(): 
            if cluster in validClusters: 
                if type=='normal':
                    data = [k for k, v in clusterInfo.iteritems() for i in range(v)]
                    mean, std = np.mean(data), np.std(data)
                    if std!=0: plotNorm(sum(data), mean, std, color=clusterMap[cluster])
                elif type=='scatter': plt.scatter(clusterInfo.keys(), clusterInfo.values(), color=clusterMap[cluster], label=cluster)
        plt.title('%s (%s)'%(location['name'],location['location'])),plt.legend()
#        plt.show()
        plt.xlim(xmin=0,xmax=24)
        plt.savefig(fileName), plt.clf()
#    for clustering in iteraterUserClusterings(place):
    for location in locationToUserMapIterator(place, minCheckins=place['minLocationCheckinsForPlots']): 
        print clustering[0], location['location']
        fileName=placesImagesFolder%place['name']+'%s/'%type+str(clustering[0])+'/'+ location['location'].replace(' ', '_').replace('.', '+')+'.png'
        FileIO.createDirectoryForFile(fileName)
        scatterPlot(clustering, location, fileName)

def writeUserClusterKMLs(place):
    clustering = getBestUserClustering(place)
    colorMap = clustering[3]
    for clusterId, details in sorted(getUserClusteringDetails(place, clustering).iteritems(), key=lambda k: int(k[0])):
        kml = SpotsKML()
        kml.addLocationPointsWithTitles([(getLocationFromLid(lid), unicode(name).encode('utf-8')) for lid, name, _ in details['locations'][:5]], color=colorMap[clusterId])
        outputKMLFile=placesKMLsFolder%place['name']+'locations/userClusters/%s/%s.kml'%(str(clustering[0]), str(clusterId))
        FileIO.createDirectoryForFile(outputKMLFile)
        kml.write(outputKMLFile)

#    userVectors = defaultdict(dict)
#    locationToUserMap = dict((l['location'], l) for l in locationToUserMapIterator(place))
#    for lid in locationToUserMap:
#        for user in locationToUserMap[lid]['users']: userVectors[user][lid]=sum(len(locationToUserMap[lid]['users'][user][d][db]) for d in locationToUserMap[lid]['users'][user] for db in locationToUserMap[lid]['users'][user][d])
#    for user in userVectors.keys()[:]: 
#        if sum(userVectors[user].itervalues())<place['minUserCheckins']: del userVectors[user]
#    print userVectors
#    for clustering in iteraterUserClusterings(place):
#        locationDistributionForClusters = defaultdict(dict)
#        for clusterId, users in clustering[2].iteritems():
#            for user in users:
#                for lid, count in userVectors[user].iteritems():
#                    if lid not in locationDistributionForClusters[clusterId]: locationDistributionForClusters[clusterId][lid]=0
#                    locationDistributionForClusters[clusterId][lid]+=count
#        colorMap = clustering[3]
#        for clusterId, locationDistributionForClusters in locationDistributionForClusters.iteritems(): 
#            kml = SpotsKML()
#            kml.addLocationPointsWithTitles([(getLocationFromLid(p[0]), locationToUserMap[p[0]]['name']) for p in  sorted(locationDistributionForClusters.iteritems(), key=itemgetter(1), reverse=True)[:5]], color=colorMap[clusterId])
#            outputKMLFile=placesKMLsFolder%place['name']+'locations/%s/%s.kml'%(str(clustering[0]), str(clusterId))
#            FileIO.createDirectoryForFile(outputKMLFile)
#            kml.write(outputKMLFile)
            
def locationClusterMeansIterator(place):
    clustering = getBestUserClustering(place)
    validClusters = getUserClusteringDetails(place, clustering).keys()
    for location in locationToUserMapIterator(place, minCheckins=place['minLocationCheckinsForPlots']): 
        userClusterMap = {}
        for clusterId, users in clustering[2]['clusters'].iteritems():
            for user in users: 
                if user in location['users']: userClusterMap[user]=clusterId
        scatterData = defaultdict(dict)
        clusterMap = clustering[3]
        for user, userVector in location['users'].iteritems():
            if user in userClusterMap:
                for d in userVector:
                    for db in userVector[d]:
                        for h in [(datetime.datetime.fromtimestamp(ep).hour-6)%24 for ep in userVector[d][db]]:
                            if h not in scatterData[userClusterMap[user]]: scatterData[userClusterMap[user]][h]=0
                            scatterData[userClusterMap[user]][h]+=1
        allData = [k for cluster, clusterInfo in scatterData.iteritems() for k, v in clusterInfo.iteritems() for i in range(v)]
        locationData = []
        for cluster, clusterInfo in scatterData.iteritems(): 
            if cluster in validClusters: 
                data = [k for k, v in clusterInfo.iteritems() for i in range(v)]
                locationData.append((cluster, np.mean(data), np.std(data)))
        yield {'combined': [np.mean(allData), np.std(allData)], 'clusters': locationData}

def plotClusterDistributionInLocations(place):
    clusterCount, distribution = getDataDistribution([len(location['clusters']) for location in locationClusterMeansIterator(place)])
    plt.plot(clusterCount, distribution)
    plt.show()
def plotClusterOverlapInLocations(place):
    for location in locationClusterMeansIterator(place):
        if len(location['clusters'])>=2:
            for cluster1, cluster2 in combinations(location['clusters'], 2):
                mu1, mu2, sd1, sd2 = cluster1[1], cluster2[1], cluster1[2], cluster2[2]
#                print cluster1, cluster2
                print mu1, mu2, sd1, sd2, getWeitzmanOVL(mu1, mu2, sd1, sd2)
    
def getUserClusterDetails(place):
    for clusterId, details in sorted(getUserClusteringDetails(place, getBestUserClustering(place)).iteritems(), key=lambda k: int(k[0])):
        print clusterId, len(details['users']), [t[1] for t in details['locations'][:5]]
    
#place = {'name':'brazos', 'boundary':brazos_valley_boundary, 'minUserCheckins':10, 'minLocationCheckins': 0}
place = {'name':'austin_tx', 'boundary':austin_tx_boundary, 'minUserCheckins':5, 'minLocationCheckinsForPlots': 50, 'maxLocationCheckinsForPlots': (), 'minimunUsersInUserCluster': 20, 'minLocationCheckins': 0}

#writeLocationToUserMap(place)
#writeUserClusters(place)
#getUserClusterDetails(place)

#writeLocationsWithClusterInfoFile(place)
#writeLocationClusters(place)


#writeUserClusterKMLs(place)

#getLocationsCheckinDistribution(place)
#getLocationDistributionPlots(place)
#getLocationPlots(place)
#getLocationPlots(place, type='normal')

#plotClusterDistributionInLocations(place)
plotClusterOverlapInLocations(place)

#print len(list(locationToUserMapIterator(place)))
#print len(list(locationToUserMapIterator(place,minCheckins=100)))


