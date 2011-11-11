'''
Created on Nov 10, 2011

@author: kykamath
'''
import sys
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
    dallas_tx_boundary
from library.weka import Clustering, ARFF
from operator import itemgetter
from library.clustering import getTopFeaturesForClass

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
        
def getUserVectors(place):
    userVectors = defaultdict(dict)
    locationToUserMap = dict((l['location'], l) for l in locationToUserMapIterator(place, minCheckins=50))
    for lid in locationToUserMap:
        for user in locationToUserMap[lid]['users']: 
            userVectors[user][lid.replace(' ', '_')]=sum(len(locationToUserMap[lid]['users'][user][d][db]) for d in locationToUserMap[lid]['users'][user] for db in locationToUserMap[lid]['users'][user][d])
    for user in userVectors.keys()[:]: 
        if sum(userVectors[user].itervalues())<place['minUserCheckins']: del userVectors[user]
    return userVectors
 
def writeARFFFile(place):
    userVectors = getUserVectors(place)
    arffFile=ARFF.writeARFFForClustering(userVectors, place['name'])
    outputFileName = placesARFFFile%place['name']
    FileIO.createDirectoryForFile(outputFileName)
    GeneralMethods.runCommand('mv %s %s'%(arffFile, outputFileName))

def writeUserClustersFile(place):
    print 'Generating clusters...'
    userVectors = getUserVectors(place)
    GeneralMethods.runCommand('rm -rf %s'%placesUserClustersFile%place['name'])
    clusterAssignments = Clustering.cluster(Clustering.EM, placesARFFFile%place['name'], userVectors, '-N -1')
#    clusterAssignments = Clustering.cluster(Clustering.KMeans, placesARFFFile%place['name'], userVectors, '-N 2')
    for userId, userVector in userVectors.iteritems(): userVectors[userId] = {'userVector': userVector, 'clusterId': clusterAssignments[userId]}
    for data in userVectors.iteritems(): FileIO.writeToFileAsJson(data, placesUserClustersFile%place['name'])
    
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
#    writeTopFeaturesForCluster()
    writeClusterKML()

#place = {'name':'brazos', 'boundary':brazos_valley_boundary, 'minUserCheckins':5}
#place = {'name':'austin_tx', 'boundary':austin_tx_boundary,'minUserCheckins':5}
place = {'name': 'dallas_tx', 'boundary': dallas_tx_boundary, 'minUserCheckins':5}

writeLocationToUserMap(place)
writeARFFFile(place)
writeUserClustersFile(place)
writeTopClusterFeatures(place)
analyzeClusters(place)