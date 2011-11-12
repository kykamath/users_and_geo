'''
Created on Nov 10, 2011

@author: kykamath
'''
import sys, datetime, random, os
from library.plotting import plotNorm
sys.path.append('../')
from analysis import SpotsKML
from collections import defaultdict
from library.classes import GeneralMethods
from analysis.mr_analysis import filteredLocationToUserAndTimeMapIterator
from library.geo import getLocationFromLid, isWithinBoundingBox
from mongo_settings import venuesCollection, venuesMetaDataCollection
from library.file_io import FileIO
from settings import brazos_valley_boundary, placesLocationToUserMapFile,\
    minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation,\
    locationToUserAndExactTimeMapFile, placesARFFFile, placesUserClustersFile,\
    placesUserClusterFeaturesFile, austin_tx_boundary, placesAnalysisFolder,\
    dallas_tx_boundary, north_ca_boundary, placesGaussianImagesFolder
from library.weka import Clustering, ARFF
import matplotlib.pyplot as plt
from operator import itemgetter
from itertools import combinations
import numpy as np
from library.clustering import getTopFeaturesForClass

def getLocationType(location):
    classBasedOnNoOfUsers = {'id': 'users', 'low': 50, 'high': 75}
#    classBasedOnNoOfClusters = {'id': 'clusters', 'low': 2, 'high': 4}
    classBasedOnMeanDifference = {'id': 'timeMd', 'low': 2, 'high': 5}
    means = [data['mean'] for data in location['clustersInfo'].values()]
    if len(means)>1: locationInfo = {'timeMd':  np.mean([np.abs((mu1-mu2)) for mu1, mu2 in combinations(means, 2)]), 'users': len(location['users']), 'clusters': len(location['clustersInfo'])}
    else: locationInfo = {'timeMd':  0, 'users': len(location['users']), 'clusters': len(location['clustersInfo'])}
    label = ''
    for classInfo in [classBasedOnNoOfUsers, classBasedOnMeanDifference]:
        classId, classLow, classHigh = classInfo['id'], classInfo['low'], classInfo['high']
        type = 'mid'
        if locationInfo[classId]<=classLow: type = 'low'
        elif locationInfo[classId]>=classHigh: type = 'high'
        label+='_%s-%s_'%(classId, type)
    return label
        
    

class GenerateDataFiles:
    @staticmethod
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
    @staticmethod
    def locationToUserMapIterator(place, minCheckins=0, maxCheckins=()): 
        for location in FileIO.iterateJsonFromFile(placesLocationToUserMapFile%place['name']):
            if location['noOfCheckins']<maxCheckins and location['noOfCheckins']>=minCheckins: yield location
    @staticmethod
    def getUserVectors(place):
        userVectors = defaultdict(dict)
        locationToUserMap = dict((l['location'], l) for l in GenerateDataFiles.locationToUserMapIterator(place, minCheckins=50))
        for lid in locationToUserMap:
            for user in locationToUserMap[lid]['users']: 
                userVectors[user][lid.replace(' ', '_')]=sum(len(locationToUserMap[lid]['users'][user][d][db]) for d in locationToUserMap[lid]['users'][user] for db in locationToUserMap[lid]['users'][user][d])
        for user in userVectors.keys()[:]: 
            if sum(userVectors[user].itervalues())<place['minUserCheckins']: del userVectors[user]
        return userVectors
    @staticmethod
    def writeARFFFile(place):
        userVectors = GenerateDataFiles.getUserVectors(place)
        arffFile=ARFF.writeARFFForClustering(userVectors, place['name'])
        outputFileName = placesARFFFile%place['name']
        FileIO.createDirectoryForFile(outputFileName)
        GeneralMethods.runCommand('mv %s %s'%(arffFile, outputFileName))
    @staticmethod
    def writeUserClustersFile(place):
        print 'Generating clusters...'
        userVectors = GenerateDataFiles.getUserVectors(place)
        GeneralMethods.runCommand('rm -rf %s'%placesUserClustersFile%place['name'])
        clusterAssignments = Clustering.cluster(Clustering.EM, placesARFFFile%place['name'], userVectors, '-N -1')
    #    clusterAssignments = Clustering.cluster(Clustering.KMeans, placesARFFFile%place['name'], userVectors, '-N 2')
        for userId, userVector in userVectors.iteritems(): userVectors[userId] = {'userVector': userVector, 'clusterId': clusterAssignments[userId]}
        for data in userVectors.iteritems(): FileIO.writeToFileAsJson(data, placesUserClustersFile%place['name'])

class Analysis:
    @staticmethod
    def writeTopClusterFeatures(place):
        locationNames = {}
        def getLocationName(lid): 
            if lid not in locationNames:
                locationObject = venuesCollection.find_one({'lid':lid})
                if locationObject: locationNames[lid] = unicode(locationObject['n']).encode("utf-8")
                else: locationNames[lid] = ''
            return locationNames[lid]
        GeneralMethods.runCommand('rm -rf %s'%placesUserClusterFeaturesFile%place['name'])
        documents = [userVector.values() for user, userVector in FileIO.iterateJsonFromFile(placesUserClustersFile%place['name'])]
        for data in getTopFeaturesForClass(documents, 1000): 
            clusterId, features = data
            modifiedFeatures = []
            for feature in features: modifiedFeatures.append(list(feature) + [getLocationName(feature[0].replace('_', ' '))])
            FileIO.writeToFileAsJson([clusterId, GeneralMethods.getRandomColor(), modifiedFeatures], placesUserClusterFeaturesFile%place['name'])
    @staticmethod
    def analyzeClusters(place, noOfFeatures=10):
        def writeTopFeaturesForCluster():
            clustersFileName = '%s/topFeaturesForCluster'%placesAnalysisFolder%place['name']
            GeneralMethods.runCommand('rm -rf %s'%clustersFileName)
            for data in FileIO.iterateJsonFromFile(placesUserClusterFeaturesFile%place['name']):
                clusterId, color, features = data
                FileIO.writeToFileAsJson([clusterId, [f[2] for f in features[:noOfFeatures]]], clustersFileName)
        def writeClusterKML():
            kml = SpotsKML()
            outputKMLFile='%s/clusters.kml'%placesAnalysisFolder%place['name']
            for data in FileIO.iterateJsonFromFile(placesUserClusterFeaturesFile%place['name']):
                clusterId, color, features = data
                kml.addLocationPointsWithTitles([(getLocationFromLid(f[0].replace('_', ' ')), f[2]) for f in features[:noOfFeatures]], color=color)
                FileIO.createDirectoryForFile(outputKMLFile)
                kml.write(outputKMLFile)
        writeTopFeaturesForCluster()
        writeClusterKML()
    @staticmethod
    def iterateLocationsWithClusterDetails(place):
        userToClusterMap = dict((cluster[0],cluster[1]['clusterId']) for cluster in FileIO.iterateJsonFromFile(placesUserClustersFile%place['name']))
        clusterColorMap = dict((cluster[0],cluster[1]) for cluster in FileIO.iterateJsonFromFile(placesUserClusterFeaturesFile%place['name']))
        for location in GenerateDataFiles.locationToUserMapIterator(place): 
            clusterIdHourDist = {}
            for clusterId, hour in [(userToClusterMap[user], (datetime.datetime.fromtimestamp(epoch).hour-6)%24) for user, userVector in location['users'].iteritems() 
                   if user in userToClusterMap for day, dayVector in location['users'][user].iteritems() 
                   for db, epochs in location['users'][user][day].iteritems() for epoch in epochs]:
                        if clusterId not in clusterIdHourDist: clusterIdHourDist[clusterId]=defaultdict(int)
                        clusterIdHourDist[clusterId][hour]+=1
            total = float(sum([k for cluster, clusterInfo in clusterIdHourDist.iteritems() for k, v in clusterInfo.iteritems() for i in range(v)]))
            clustersInfo = {}
            for clusterId, distributions in clusterIdHourDist.iteritems():
                data = [k for k, v in distributions.iteritems() for i in range(v)]
                mean, std = np.mean(data), np.std(data)
                clustersInfo[clusterId]= {'mean':mean, 'std':std, 'clusterSum': sum(data), 'color': clusterColorMap[clusterId]}
            location['clustersInfo'] = clustersInfo
            location['total'] = total
            yield location
            
class PlotStats:
    @staticmethod
    def plotMeanDifferenceDistribution(place):
        dataX = []
        for location in Analysis.iterateLocationsWithClusterDetails(place):
            means = [data['mean'] for data in location['clustersInfo'].values()]
            if len(means)>1: dataX.append(np.mean([np.abs((mu1-mu2)) for mu1, mu2 in combinations(means, 2)]))
        plt.hist(dataX, bins=50)
        plt.show()
    @staticmethod
    def plotNoOfClustersDistribution(place):
        dataX = []
        for location in Analysis.iterateLocationsWithClusterDetails(place): dataX.append(len(location['clustersInfo']))
        plt.hist(dataX); plt.xlabel('Clusters'); plt.ylabel('No. of locations')
        plt.show()
    @staticmethod
    def plotMeanDifferenceVsNoOfUniqueUsers(place):
        dataX, dataY = [], []
        for location in Analysis.iterateLocationsWithClusterDetails(place):
            means = [data['mean'] for data in location['clustersInfo'].values()]
            if len(means)>1: dataX.append(len(location['users'])), dataY.append(np.mean([np.abs((mu1-mu2)) for mu1, mu2 in combinations(means, 2)]))
        plt.scatter(dataX, dataY)
        plt.xlabel('No. of unique users'), plt.ylabel('Mean difference')
        plt.show()
    @staticmethod
    def plotNoOfClustersVsNoOfUniqueUsers(place):
        dataX, dataY = [], []
        for location in Analysis.iterateLocationsWithClusterDetails(place): dataX.append(len(location['users'])), dataY.append(len(location['clustersInfo']))
        plt.scatter(dataY, dataX)
        plt.xlabel('No. of unique users'), plt.ylabel('No. of clusters')
        plt.show()
    @staticmethod
    def plotMeanDifferenceVsNoOfClusters(place):
        dataX, dataY = [], []
        for location in Analysis.iterateLocationsWithClusterDetails(place): 
            means = [data['mean'] for data in location['clustersInfo'].values()]
            if len(means)>1: dataX.append(len(location['clustersInfo'])), dataY.append(np.mean([np.abs((mu1-mu2)) for mu1, mu2 in combinations(means, 2)]))
        plt.scatter(dataY, dataX)
        plt.ylabel('No. of clusters'), plt.xlabel('Mean difference')
        plt.show()
    @staticmethod
    def plotGaussianGraphsForClusters(place):
        for location in Analysis.iterateLocationsWithClusterDetails(place):
            total = location['total']
            clustersInfo = location['clustersInfo']
            for clusterId, data in clustersInfo.iteritems():
                mean, std, clusterSum, color = data['mean'], data['std'], data['clusterSum'], data['color']
                if std!=0: plotNorm(clusterSum/total, mean, std, color=color, label=str(clusterId))
                else: plotNorm(clusterSum/total, mean, random.uniform(0.1, 0.5), color=color, label=str(clusterId))
            plt.xlim(xmin=0, xmax=23); plt.legend()
            plt.title(location['name'])
            fileName = '/'.join([placesGaussianImagesFolder%place['name'], getLocationType(location), location['location'].replace(' ', '_').replace('.', '+')+'.png'])
            print fileName
            FileIO.createDirectoryForFile(fileName)
            plt.savefig(fileName), plt.clf()
#            plt.show()
            
#place = {'name':'brazos', 'boundary':brazos_valley_boundary, 'minUserCheckins':5}
#place = {'name':'austin_tx', 'boundary':austin_tx_boundary,'minUserCheckins':5}
#place = {'name': 'dallas_tx', 'boundary': dallas_tx_boundary, 'minUserCheckins':5}
place = {'name': 'north_ca', 'boundary': north_ca_boundary, 'minUserCheckins':5}

#GenerateDataFiles.writeLocationToUserMap(place)
#GenerateDataFiles.writeARFFFile(place)
#GenerateDataFiles.writeUserClustersFile(place)

#Analysis.writeTopClusterFeatures(place)
#Analysis.analyzeClusters(place)

#for l in Analysis.iterateLocationsWithClusterDetails(place): print unicode(l['name']).encode('utf-8'), getLocationType(l)

#PlotStats.plotMeanDifferenceDistribution(place)
#PlotStats.plotNoOfClustersDistribution(place)
#PlotStats.plotMeanDifferenceVsNoOfUniqueUsers(place)
#PlotStats.plotNoOfClustersVsNoOfUniqueUsers(place)
#PlotStats.plotMeanDifferenceVsNoOfClusters(place)
PlotStats.plotGaussianGraphsForClusters(place)
