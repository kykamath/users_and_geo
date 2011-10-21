'''
Created on Oct 13, 2011

@author: kykamath
Why not geo?
=> Does not work in dense areas. New York?
=> Doesn't discover regions related but apart? Like discover xxx|yyy|zz instead of xxx|yyy|xx 

'''
import sys
sys.path.append('../')
from library.file_io import FileIO
from analysis.spots_by_locations_fi import Mahout
from settings import spotsRadiusFolder, minimumLocationsPerSpot,\
    minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation,\
    radiusInMiles, spotsUserGraphsFolder, graphNodesDistanceInMiles,\
    spotsFrequentItemsFolder, minSupport, itemsetsMergeThreshold,\
    filteredLocationToUserAndTimeMap_20_10
from analysis import Spots, SpotsKML
from mongo_settings import locationsCollection, venuesCollection,\
    locationToLocationCollection
from library.geo import getLocationFromLid, convertMilesToRadians,\
    getHaversineDistanceForLids
from library.graphs import clusterUsingMCLClustering
import networkx as nx
from analysis.mr_analysis import locationsForUsIterator, filteredUserIterator
from operator import itemgetter
from library.clustering import MultistepItemsetClustering

def getKMLForCluster(cluster):
    clusterToYield = []
    if len(cluster)>3: 
        for lid in cluster:
            title = venuesCollection.find_one({'lid':lid})
            if title!=None: clusterToYield.append((getLocationFromLid(lid), unicode(title['n']).encode("utf-8")))
            else: clusterToYield.append((getLocationFromLid(lid), ''))
    return clusterToYield

class RadiusSpots:
    @staticmethod
    def getSpotsFile(): return '%s/%s_%s_%s'%(spotsRadiusFolder, minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation, radiusInMiles)
    @staticmethod
    def iterateSpots():
        def nearbyLocations(lid, radiusInMiles): return (location for location in locationsCollection.find({"l": {"$within": {"$center": [getLocationFromLid(lid), convertMilesToRadians(radiusInMiles)]}}}))
        graph = nx.Graph()
        for lid in locationsForUsIterator(minUniqueUsersCheckedInTheLocation):
            for location in nearbyLocations(lid, radiusInMiles): graph.add_edge(location['_id'], lid)
        for locations in nx.connected_components(graph): 
            if len(locations)>=minimumLocationsPerSpot: yield getKMLForCluster(locations)
    @staticmethod
    def writeToFile(): Spots.writeSpotsToFile(RadiusSpots.iterateSpots(), RadiusSpots.getSpotsFile())
    @staticmethod
    def writeAsKML(): SpotsKML.drawKMLsForSpotsWithPoints(RadiusSpots.iterateSpots(), '%s.kml'%(RadiusSpots.getSpotsFile()), title=True)
    @staticmethod
    def writeUserDistribution(): Spots.assignUserToSpots(RadiusSpots.getSpotsFile(), filteredUserIterator(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation,  fullRecord = True))
    @staticmethod
    def getStats(): return Spots.getStats(RadiusSpots.getSpotsFile(), filteredUserIterator(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation,  fullRecord = True))
    @staticmethod
    def run():
        RadiusSpots.writeAsKML()
        RadiusSpots.writeToFile()
        RadiusSpots.writeUserDistribution()
#        print RadiusSpots.getStats()
        
class UserGraphSpots:
    @staticmethod
    def getSpotsFile(): return '%s/%s_%s_%s'%(spotsUserGraphsFolder, minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation, graphNodesDistanceInMiles)
    @staticmethod
    def iterateSpots():
        locationsToCheck = set(list(locationsForUsIterator(minUniqueUsersCheckedInTheLocation)))
        graph = nx.Graph()
        for e in locationToLocationCollection.find():
            d = e['_id'].split()
            l1, l2 = ' '.join(d[:2]), ' '.join(d[2:])
            if l1 in locationsToCheck and l2 in locationsToCheck and e['d']<=graphNodesDistanceInMiles: graph.add_edge(l1.replace(' ', '_'), l2.replace(' ', '_'), {'w': e['u']})
        for locations in nx.connected_components(graph): 
            if len(locations)>=minimumLocationsPerSpot: 
                clusters = clusterUsingMCLClustering(graph.subgraph(locations), inflation=20)
                print graph.subgraph(locations).number_of_nodes(), graph.subgraph(locations).number_of_edges(), len(clusters)
                for cluster in clusters: 
                    if len(cluster)>=minimumLocationsPerSpot:  yield getKMLForCluster([c.replace('_', ' ') for c in cluster])
    @staticmethod
    def writeToFile(): Spots.writeSpotsToFile(UserGraphSpots.iterateSpots(), UserGraphSpots.getSpotsFile())
    @staticmethod
    def writeAsKML(): SpotsKML.drawKMLsForSpotsWithPoints(UserGraphSpots.iterateSpots(), '%s.kml'%(UserGraphSpots.getSpotsFile()), title=True)
    @staticmethod
    def writeUserDistribution(): Spots.writeUserDistributionInSpots(UserGraphSpots.getSpotsFile(), filteredUserIterator(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation,  fullRecord = True))
    @staticmethod
    def getStats(): return Spots.getStats(UserGraphSpots.getSpotsFile(), filteredUserIterator(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation,  fullRecord = True))
    @staticmethod
    def run():
        UserGraphSpots.writeAsKML()
        UserGraphSpots.writeToFile()
        UserGraphSpots.writeUserDistribution()
        print UserGraphSpots.getStats()

class FrequentItemSpots:        
    @staticmethod
    def getSpotsFile(): return '%s/%s_%s_%s'%(spotsFrequentItemsFolder, minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation, itemsetsMergeThreshold)
    @staticmethod
    def iterateSpots():
        def iterateItemsets():
            for itemset, support in sorted(Mahout.iterateFrequentLocationsFromFIMahout(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation, minSupport, yieldSupport=True, lids=True), key=itemgetter(1), reverse=True):
                if len(itemset)>=2: yield itemset
        for cluster in MultistepItemsetClustering().cluster(iterateItemsets(), getHaversineDistanceForLids, itemsetsMergeThreshold):
            if len(cluster)>minimumLocationsPerSpot: yield getKMLForCluster(cluster)
    @staticmethod
    def writeAsKML(): SpotsKML.drawKMLsForSpotsWithPoints(FrequentItemSpots.iterateSpots(), '%s.kml'%(FrequentItemSpots.getSpotsFile()), title=True)
    @staticmethod
    def writeToFile(): Spots.writeSpotsToFile(FrequentItemSpots.iterateSpots(), FrequentItemSpots.getSpotsFile())
    @staticmethod
    def writeUserDistribution(): Spots.writeUserDistributionInSpots(FrequentItemSpots.getSpotsFile(), filteredUserIterator(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation,  fullRecord = True))
    @staticmethod
    def getStats(): return Spots.getStats(FrequentItemSpots.getSpotsFile(), filteredUserIterator(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation,  fullRecord = True))
    @staticmethod
    def run():
        FrequentItemSpots.writeAsKML()
        FrequentItemSpots.writeToFile()
        FrequentItemSpots.writeUserDistribution()
        print FrequentItemSpots.getStats()
        
from collections import defaultdict
class ItemsetClustering:
    def __init__(self):
        items = {}
        clusters = defaultdict(set)
        clusterOverlapMappings = defaultdict(set)
        clusterOverlaps = defaultdict(set)
    def addItem(self, item, clusterId): pass
    def addItemsToNewCluster(self, items): pass
    def addItems(self, items, clusterId): pass
    def noteItemOverlaps(self, clusterId1, clusterId2, items): pass
    def mergeCluster(self, clusterId1, clusterId2): pass
#    def removeCluster(self, clusterId): pass

if __name__ == '__main__':
#    RadiusSpots.run()
#    UserGraphSpots.run()
#    FrequentItemSpots.run()
#    i = 0
#    for data in FileIO.iterateJsonFromFile('../data/spots'):
#        if len(data['users'])>10 and len(data['users'])<1000: i+=1
#        print len(data['users'])
#    print i
#    filteredLocationToUserAndTimeMap_20_10
