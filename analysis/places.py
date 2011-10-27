'''
model iwth probabilites

overlap and variance from mean.

Lot of clusters, very little overlap
Lot of clusters, high overlap
Few clusters, very little overlap
Few clusters, high overlap

Created on Oct 21, 2011

@author: kykamath
'''
import sys, datetime, random, math
from scipy.stats.stats import pearsonr
sys.path.append('../')
from library.vector import Vector
from library.classes import GeneralMethods
from library.plotting import getDataDistribution, plotNorm, getLatexForString
from library.stats import getWeitzmanOVL
from analysis import SpotsKML
from library.clustering import KMeansClustering
from library.plotting import getDataDistribution
from library.file_io import FileIO
from mongo_settings import venuesCollection, venuesMetaDataCollection
from analysis.mr_analysis import locationIterator,\
    filteredLocationToUserAndTimeMapIterator
from library.geo import isWithinBoundingBox, getLocationFromLid,\
    getLidFromLocation
from settings import brazos_valley_boundary, minUniqueUsersCheckedInTheLocation,\
    minLocationsTheUserHasCheckedin, placesLocationToUserMapFile,\
    placesImagesFolder, locationToUserAndExactTimeMapFile,\
    austin_tx_boundary, placesKMLsFolder, placesAnalysisFolder,\
    placesLocationWithClusterInfoFile, placesUserClustersFile,\
    placesLocationClustersFile, dallas_tx_boundary, north_ca_boundary
from collections import defaultdict
from itertools import groupby, combinations
from operator import itemgetter
import numpy as np
import matplotlib.pyplot as plt

CLUSTERS_OVL_TYPE_LOW_LOW = 'low_low'
CLUSTERS_OVL_TYPE_LOW_HIGH = 'low_high' 
CLUSTERS_OVL_TYPE_HIGH_LOW = 'high_low' 
CLUSTERS_OVL_TYPE_HIGH_HIGH = 'high_high'     

HIGH_OVL_LOW_MEAN_DIFF = 'h_o_l_md'
HIGH_OVL_HIGH_MEAN_DIFF = 'h_o_h_md'
LOW_OVL_LOW_MEAN_DIFF = 'l_o_l_md'
LOW_OVL_HIGH_MEAN_DIFF = 'l_o_h_md'

def meanClusteringDistance(clusterMeans): return np.mean([Vector.euclideanDistance(Vector(dict(c1)), Vector(dict(c2))) for c1, c2 in combinations(clusterMeans,2)])

def writeLocationToUserMap(place):
    name, boundary = place['name'], place['boundary']
    GeneralMethods.runCommand('rm -rf %s'%placesLocationToUserMapFile%name)
    for location in filteredLocationToUserAndTimeMapIterator(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation, inputFile=locationToUserAndExactTimeMapFile):
        lid=getLocationFromLid(location['location'])
        if isWithinBoundingBox(lid, boundary): 
            location['categories'] = ''; location['tags'] = ''; location['name']=''
            title = venuesCollection.find_one({'lid':location['location']})
            if title: location['name'] = unicode(title['n']).encode("utf-8")
            meta = venuesMetaDataCollection.find_one({'_id':location['location']})
            if meta: location['categories'] = unicode(meta['c']).encode("utf-8"); location['tags'] = unicode(meta['t']).encode("utf-8")
            for user in location['users'].keys()[:]: location['users'][str(user)]=location['users'][user]; del location['users'][user]
            location['noOfCheckins']=sum([len(epochs) for user, userVector in location['users'].iteritems() for day, dayVector in userVector.iteritems() for db, epochs in dayVector.iteritems()])
            if location['noOfCheckins']>place.get('minLocationCheckins',0): FileIO.writeToFileAsJson(location, placesLocationToUserMapFile%name)
def locationToUserMapIterator(place, minCheckins=0, maxCheckins=()): 
    for location in FileIO.iterateJsonFromFile(placesLocationToUserMapFile%place['name']):
        if location['noOfCheckins']<maxCheckins and location['noOfCheckins']>=minCheckins: yield location
  
#def writeUserClusters(place):
#    numberOfTopFeatures = 10000
#    GeneralMethods.runCommand('rm -rf %s'%placesUserClustersFile%place['name'])
#    userVectors = defaultdict(dict)
#    locationToUserMap = dict((l['location'], l) for l in locationToUserMapIterator(place, minCheckins=50))
#    for lid in locationToUserMap:
#        for user in locationToUserMap[lid]['users']: 
#            userVectors[user][lid.replace(' ', '_')]=sum(len(locationToUserMap[lid]['users'][user][d][db]) for d in locationToUserMap[lid]['users'][user] for db in locationToUserMap[lid]['users'][user][d])
#    for user in userVectors.keys()[:]: 
#        if sum(userVectors[user].itervalues())<place['minUserCheckins']: del userVectors[user]
#    resultsForVaryingK = []
#    for k in range(4, 100):
#        try:
#            print 'Clustering with k=%s'%k
#            clusters = KMeansClustering(userVectors.iteritems(), k, documentsAsDict=True).cluster(normalise=True, assignAndReturnDetails=True, repeats=5, numberOfTopFeatures=numberOfTopFeatures, algorithmSource='biopython')
#            error=clusters['error']
#            for clusterId, features in clusters['bestFeatures'].items()[:]: clusters['bestFeatures'][str(clusterId)]=[(lid.replace('_', ' '), score)for lid, score in features]; del clusters['bestFeatures'][clusterId]
#            for clusterId, users in clusters['clusters'].items()[:]: clusters['clusters'][str(clusterId)]=users; del clusters['clusters'][clusterId]
#            if error: resultsForVaryingK.append((k, error, clusters, dict((clusterId, GeneralMethods.getRandomColor()) for clusterId in clusters['clusters'])))
#            else: resultsForVaryingK.append((k, meanClusteringDistance(clusters['bestFeatures'].itervalues()), clusters, dict((clusterId, GeneralMethods.getRandomColor()) for clusterId in clusters['clusters'])))
#        except Exception as e: print '*********** Exception while clustering k = %s; %s'%(k, e); pass
#    FileIO.writeToFileAsJson(min(resultsForVaryingK, key=itemgetter(1)), placesUserClustersFile%place['name'])
#    for data in resultsForVaryingK: FileIO.writeToFileAsJson(data, placesUserClustersFile%place['name'])
def writeUserClusters(place):
    numberOfTopFeatures = 10000
#    GeneralMethods.runCommand('rm -rf %s'%placesUserClustersFile%place['name'])
    userVectors = defaultdict(dict)
    locationToUserMap = dict((l['location'], l) for l in locationToUserMapIterator(place, minCheckins=50))
    for lid in locationToUserMap:
        for user in locationToUserMap[lid]['users']: 
            userVectors[user][lid.replace(' ', '_')]=sum(len(locationToUserMap[lid]['users'][user][d][db]) for d in locationToUserMap[lid]['users'][user] for db in locationToUserMap[lid]['users'][user][d])
    for user in userVectors.keys()[:]: 
        if sum(userVectors[user].itervalues())<place['minUserCheckins']: del userVectors[user]
    resultsForVaryingK = []
    for k in range(2,200):
        try:
            print 'Clustering with k=%s'%k
            clusters = KMeansClustering(userVectors.iteritems(), k, documentsAsDict=True).cluster(normalise=True, assignAndReturnDetails=True, repeats=5, numberOfTopFeatures=numberOfTopFeatures, algorithmSource='biopython')
            error=clusters['error']
            for clusterId, features in clusters['bestFeatures'].items()[:]: clusters['bestFeatures'][str(clusterId)]=[(lid.replace('_', ' '), score)for lid, score in features]; del clusters['bestFeatures'][clusterId]
            for clusterId, users in clusters['clusters'].items()[:]: clusters['clusters'][str(clusterId)]=users; del clusters['clusters'][clusterId]
            if error: 
                resultsForVaryingK.append((k, error, clusters, dict((clusterId, GeneralMethods.getRandomColor()) for clusterId in clusters['clusters'])))
                FileIO.writeToFileAsJson((k, error, clusters, dict((clusterId, GeneralMethods.getRandomColor()) for clusterId in clusters['clusters'])), placesUserClustersFile%place['name'])
            else: resultsForVaryingK.append((k, meanClusteringDistance(clusters['bestFeatures'].itervalues()), clusters, dict((clusterId, GeneralMethods.getRandomColor()) for clusterId in clusters['clusters'])))
        except Exception as e: print '*********** Exception while clustering k = %s; %s'%(k, e); pass
#    for data in resultsForVaryingK: FileIO.writeToFileAsJson(data, placesUserClustersFile%place['name'])
#def getBestUserClustering(place, idOnly=False): 
#    for data in FileIO.iterateJsonFromFile(placesUserClustersFile%place['name']): 
#        if idOnly: return data[0]
#        return data
def iteraterUserClusterings(place): 
#    i = 0
    for data in FileIO.iterateJsonFromFile(placesUserClustersFile%place['name']): yield data
#        if i!=0: yield data; 
#        i+=1
def getUserClustering(place, k):
    for clustering in iteraterUserClusterings(place):
        if k==clustering[0]: return clustering
def getUserClusteringDetails(place, clustering):
    locationNameMap = dict((location['location'], location['name'])for location in locationToUserMapIterator(place))
    clusterDetails = defaultdict(list)
    for (clusterId, users), (clusterId, features) in zip(clustering[2]['clusters'].iteritems(), clustering[2]['bestFeatures'].iteritems()):
        if len(users)>place.get('minimunUsersInUserCluster', 0): clusterDetails[clusterId] = {'users': users, 'locations': [(lid, locationNameMap[lid], score) for lid, score in features]}
    return clusterDetails
def plotCurvesToSelectk(place):
    errors, noOfClustersMap = [], {}
    for clustering in iteraterUserClusterings(place): 
        k = clustering[0]
        if k>1: errors.append(clustering[1]); noOfClustersMap[k] = [len(details['users']) for clusterId, details in getUserClusteringDetails(place, clustering).iteritems()]
    plt.subplot(3,1,1); plt.plot(errors); plt.ylabel('error')
    plt.subplot(3,1,2); plt.plot([len(i) for i in noOfClustersMap.values()]); plt.ylabel('no. of clusters')
    plt.subplot(3,1,3); plt.plot([np.std(i) for i in noOfClustersMap.values()]); plt.ylabel('no. of clusters')
    print max([(k, len(i)) for k, i in noOfClustersMap.iteritems()], key=itemgetter(1))
#    print 'k =', noOfClusters.index(max(noOfClusters))+1, '\tNo.of. clusters =', max(noOfClusters)
    plt.show()
    

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
    clusterId = place.get('k')
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

def getLocationPlots(place, clusterOVLType, type='scatter'):
    clustering = getUserClustering(place, place.get('k'))
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
                    else: plotNorm(sum(data), mean, random.uniform(0.1, 0.5), color=clusterMap[cluster])
                elif type=='scatter': plt.scatter(clusterInfo.keys(), clusterInfo.values(), color=clusterMap[cluster], label=cluster)
        plt.title('%s (%s)'%(location['name'],location['location'])),plt.legend()
#        plt.show()
        plt.xlim(xmin=0,xmax=24)
        plt.savefig(fileName), plt.clf()
#    for clustering in iteraterUserClusterings(place):
    for location in locationToUserMapIterator(place, minCheckins=place['minLocationCheckinsForPlots']): 
#    for location in iterateLocationsByOVLAndClustersType(place, clusterOVLType): 
#        location = location['details']
        print clustering[0], location['location']
        fileName=placesImagesFolder%place['name']+'%s/'%type+str(clustering[0])+'/'+ location['location'].replace(' ', '_').replace('.', '+')+'.png'
        FileIO.createDirectoryForFile(fileName)
        scatterPlot(clustering, location, fileName)
        
def getLocationPlotsByOVLAndMeanDifference(place):
    clustering = getUserClustering(place, place.get('k'))
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
        total = float(sum([k for cluster, clusterInfo in scatterData.iteritems() for k, v in clusterInfo.iteritems() for i in range(v)]))
        for cluster, clusterInfo in scatterData.iteritems(): 
            if cluster in validClusters: 
#                if type=='normal':
                data = [k for k, v in clusterInfo.iteritems() for i in range(v)]
                mean, std = np.mean(data), np.std(data)
                if std!=0: plotNorm(sum(data)/total, mean, std, color=clusterMap[cluster])
                else: plotNorm(sum(data)/total, mean, random.uniform(0.1, 0.5), color=clusterMap[cluster])
#                elif type=='scatter': plt.scatter(clusterInfo.keys(), clusterInfo.values(), color=clusterMap[cluster], label=cluster)
        plt.title('%s (%s)'%(location['name'],location['location'])),plt.legend()
#        plt.show()
        plt.xlim(xmin=0,xmax=24)
        plt.savefig(fileName), plt.clf()
#    for clustering in iteraterUserClusterings(place):
    for location, type in iterateLocationsByOVLAndMeanDifference(place): 
        location = location['details']
        print clustering[0], location['location']
        fileName=placesImagesFolder%place['name']+str(clustering[0])+'/%s/'%type+ location['location'].replace(' ', '_').replace('.', '+')+'.png'
        FileIO.createDirectoryForFile(fileName)
        scatterPlot(clustering, location, fileName)

def writeUserClusterKMLs(place):
    clustering = getUserClustering(place, place.get('k'))
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
    clustering = getUserClustering(place, place.get('k'))
    validClusters = getUserClusteringDetails(place, clustering).keys()
    locationDetailsMap = dict((l['location'], l) for l in locationToUserMapIterator(place))
    for location in locationToUserMapIterator(place, minCheckins=place.get('minLocationCheckinsForPlots',0)): 
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
        yield {'combined': [np.mean(allData), np.std(allData)], 'clusters': locationData, 'details': locationDetailsMap[location['location']]}
        

def iterateLocationsByOVLAndMeanDifference(place):        
    for location in locationClusterMeansIterator(place):
        locationMeanDifferences, locationOverlaps = [], []
        if len(location['clusters'])>=2:
            for cluster1, cluster2 in combinations(location['clusters'], 2):
                mu1, mu2, sd1, sd2 = cluster1[1], cluster2[1], cluster1[2], cluster2[2]
                val = np.abs(mu1-mu2)
                locationMeanDifferences.append(val)
                val = getWeitzmanOVL(mu1, mu2, sd1, sd2)[0]
                locationOverlaps.append(val)
        locationMeanDifferences = [x for x in locationMeanDifferences if not np.isnan(x)]
        locationOverlaps = [y for y in locationOverlaps if not np.isnan(y)]
        md = np.mean(locationMeanDifferences); ovl=len(location['clusters'])# ovl = np.mean(locationOverlaps)
        if ovl<=3 and md<=2: yield (location, LOW_OVL_LOW_MEAN_DIFF)
        elif ovl<=3 and md>6: yield (location, LOW_OVL_HIGH_MEAN_DIFF)
        elif ovl>8 and md<=2: yield (location, HIGH_OVL_LOW_MEAN_DIFF)
        elif ovl>8 and md>6: yield (location, HIGH_OVL_HIGH_MEAN_DIFF)

def plotNoOfClusersPerLocationDistribution(place):
    data = [len(location['clusters']) for location in locationClusterMeansIterator(place)]
    clusterCount, distribution = getDataDistribution(data)
    plt.plot(clusterCount, distribution)
    plt.title(getLatexForString('\# of clusters / location (\mu=%0.2f \sigma=%0.2f)'%(np.mean(data), np.std(data))))
    plt.show()

def plotOverlapDistribution(place):
    oVLValues = []
    for location in locationClusterMeansIterator(place):
        locationOVLValues = []
        if len(location['clusters'])>=2:
            for cluster1, cluster2 in combinations(location['clusters'], 2):
                mu1, mu2, sd1, sd2 = cluster1[1], cluster2[1], cluster1[2], cluster2[2]
                ovl = getWeitzmanOVL(mu1, mu2, sd1, sd2)
                locationOVLValues.append(ovl[0])
        if locationOVLValues: 
            mean = np.mean(locationOVLValues)
            if mean>0: oVLValues.append(mean)
    plt.hist(oVLValues, 100, facecolor='green', alpha=0.75)
    plt.show()
    
def plotClusterOverlapInLocations(place):
    dataX, dataY = [], []
    for location in locationClusterMeansIterator(place):
        locationOVLValues = []
        if len(location['clusters'])>=2:
            for cluster1, cluster2 in combinations(location['clusters'], 2):
                mu1, mu2, sd1, sd2 = cluster1[1], cluster2[1], cluster1[2], cluster2[2]
                ovl = getWeitzmanOVL(mu1, mu2, sd1, sd2)
                locationOVLValues.append(ovl[0])
        if locationOVLValues:
            lengthOfCluster = len(location['clusters']) 
            mean = np.mean(locationOVLValues)
            if lengthOfCluster<=place['lowClusters'] and mean<=place['lowOVL']: plt.scatter([lengthOfCluster], [mean], color='g')
            elif lengthOfCluster<=place['lowClusters'] and mean>=place['highOVL']: plt.scatter([lengthOfCluster], [mean], color='y')
            elif lengthOfCluster>=place['highClusters'] and mean<=place['lowOVL']: plt.scatter([lengthOfCluster], [mean], color='r')
            elif lengthOfCluster>=place['highClusters'] and mean>=place['highOVL']: plt.scatter([lengthOfCluster], [mean], color='m')
            else: plt.scatter([lengthOfCluster], [np.mean(locationOVLValues)], color='b')
    plt.show()

def plotClusterOverlapByLocationCheckins(place):
    dataX, dataY = [], []
    for location in locationClusterMeansIterator(place):
        locationOverlaps = []
        if len(location['clusters'])>=2:
            for cluster1, cluster2 in combinations(location['clusters'], 2):
                mu1, mu2, sd1, sd2 = cluster1[1], cluster2[1], cluster1[2], cluster2[2]
                locationOverlaps.append(getWeitzmanOVL(mu1, mu2, sd1, sd2)[0])
        dataY.append(np.mean(locationOverlaps))
        dataX.append(location['details']['noOfCheckins'])
    plt.scatter(dataX, dataY)
    plt.show()

def plotClusterMeanDifferenceByLocationCheckins(place):
    dataX, dataY = [], []
    for location in locationClusterMeansIterator(place):
        locationMeanDifferences = []
        if len(location['clusters'])>=2:
            for cluster1, cluster2 in combinations(location['clusters'], 2):
                mu1, mu2, sd1, sd2 = cluster1[1], cluster2[1], cluster1[2], cluster2[2]
                locationMeanDifferences.append(np.abs(mu1-mu2))
        dataY.append(np.mean(locationMeanDifferences))
        dataX.append(location['details']['noOfCheckins'])
    plt.scatter(dataX, dataY)
    plt.show()
    
def pltClusterOverlapHistogram(place):
    dataY = []
    for location in locationClusterMeansIterator(place):
        locationOverlaps = []
        if len(location['clusters'])>=2:
            for cluster1, cluster2 in combinations(location['clusters'], 2):
                mu1, mu2, sd1, sd2 = cluster1[1], cluster2[1], cluster1[2], cluster2[2]
                locationOverlaps.append(getWeitzmanOVL(mu1, mu2, sd1, sd2)[0])
        dataY.append(np.mean(locationOverlaps))
    plt.hist(dataY, range=(min(dataY), max(dataY)))
    plt.show()

def pltClusterMeanDifferenceHistogram(place):
    dataY = []
    for location in locationClusterMeansIterator(place):
        locationMeanDifferences = []
        if len(location['clusters'])>=2:
            for cluster1, cluster2 in combinations(location['clusters'], 2):
                mu1, mu2, sd1, sd2 = cluster1[1], cluster2[1], cluster1[2], cluster2[2]
                locationMeanDifferences.append(np.abs(mu1-mu2))
        dataY.append(np.mean(locationMeanDifferences))
    plt.hist(dataY, range=(min(dataY), max(dataY)))
    plt.show()

def pltClusterMeanDifferenceByOverlap(place):
    dataX, dataY = [], []
    for location in locationClusterMeansIterator(place):
        locationMeanDifferences, locationOverlaps = [], []
        if len(location['clusters'])>=2:
            for cluster1, cluster2 in combinations(location['clusters'], 2):
                mu1, mu2, sd1, sd2 = cluster1[1], cluster2[1], cluster1[2], cluster2[2]
                val = np.abs(mu1-mu2)
                locationMeanDifferences.append(val)
                val = getWeitzmanOVL(mu1, mu2, sd1, sd2)[0]
                locationOverlaps.append(val)
        dataY.append(np.mean(locationMeanDifferences))
#        dataX.append(np.mean(locationOverlaps))
        dataX.append(len(location['clusters']))
#    print np.isnan(dataX[144])
    print len(dataX), len(dataY)
    newDataX, newDataY = [], []
    [(newDataX.append(x), newDataY.append(y)) for x,y in zip(dataX, dataY) if not np.isnan(x)]
#    dataX = [x for x, in dataX if not np.isnan(x)]
#    dataY = [y for y in dataY if not np.isnan(y)]
    print len(newDataX), len(newDataY)
    plt.scatter(newDataX, newDataY)
#    print pearsonr(dataX, dataY)
#    plt.title(str(pearsonr(dataX, dataY)))
    plt.show()

def iterateLocationsByOVLAndClustersType(place, type):
    dataX, dataY = [], []
    for location in locationClusterMeansIterator(place):
        locationOVLValues = []
        if len(location['clusters'])>=2:
            for cluster1, cluster2 in combinations(location['clusters'], 2):
                mu1, mu2, sd1, sd2 = cluster1[1], cluster2[1], cluster1[2], cluster2[2]
                ovl = getWeitzmanOVL(mu1, mu2, sd1, sd2)
                locationOVLValues.append(ovl[0])
        if locationOVLValues:
            lengthOfCluster = len(location['clusters']) 
            mean = np.mean(locationOVLValues)
            locationType = None
            if lengthOfCluster<=place['lowClusters'] and mean<=place['lowOVL']: locationType = CLUSTERS_OVL_TYPE_LOW_LOW
            elif lengthOfCluster<=place['lowClusters'] and mean>=place['highOVL']: locationType = CLUSTERS_OVL_TYPE_LOW_HIGH
            elif lengthOfCluster>=place['highClusters'] and mean<=place['lowOVL']: locationType = CLUSTERS_OVL_TYPE_HIGH_LOW
            elif lengthOfCluster>=place['highClusters'] and mean>=place['highOVL']: locationType = CLUSTERS_OVL_TYPE_HIGH_HIGH
            if locationType==type: yield location

def getUserClusterDetails(place):
    clustering = getUserClustering(place, place.get('k'))
    for clusterId, details in sorted(getUserClusteringDetails(place, clustering).iteritems(), key=lambda k: int(k[0])):
        print clusterId, len(details['users']), [t[1] for t in details['locations'][:5]]

#place = {'name':'brazos', 'boundary':brazos_valley_boundary, 'minUserCheckins':10, 'minLocationCheckins': 0}

#place = {'name':'austin_tx', 'boundary':austin_tx_boundary, 'k': 83,'minUserCheckins':5, 'minLocationCheckinsForPlots': 50, 'maxLocationCheckinsForPlots': (), 'minimunUsersInUserCluster': 20, 
#         'minLocationCheckins': 0, 'lowClusters': 6, 'highClusters': 12, 'lowOVL': 0.15, 'highOVL':0.4}
#103
#place = {'name': 'dallas_tx', 'boundary': dallas_tx_boundary, 'k':103, 'minUserCheckins':5, 'minimunUsersInUserCluster': 15, 'lowClusters': 2, 'highClusters': 6, 'lowOVL': 0.1, 'highOVL':0.4,
#         'minLocationCheckinsForPlots': 50}

place = {'name': 'north_ca', 'boundary': north_ca_boundary, 'minUserCheckins':5 }

#writeLocationToUserMap(place)
writeUserClusters(place)
#plotCurvesToSelectk(place)
#getUserClusterDetails(place)

#writeLocationsWithClusterInfoFile(place)
#writeLocationClusters(place)


#writeUserClusterKMLs(place)

#getLocationsCheckinDistribution(place)
#getLocationDistributionPlots(place)
#getLocationPlots(place)
#getLocationPlots(place, CLUSTERS_OVL_TYPE_LOW_HIGH, type='normal')
#getLocationPlotsByOVLAndMeanDifference(place)

#plotNoOfClusersPerLocationDistribution(place)
#plotOverlapDistribution(place)
#plotClusterOverlapInLocations(place)

#plotClusterOverlapByLocationCheckins(place)
#plotClusterMeanDifferenceByLocationCheckins(place)

#pltClusterOverlapHistogram(place)
#pltClusterMeanDifferenceHistogram(place)
#pltClusterMeanDifferenceByOverlap(place)

