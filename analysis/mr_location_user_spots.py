'''
Created on Oct 13, 2011

@author: kykamath
'''
import sys
sys.path.append('../')
from settings import spotsRadiusFolder, minimumLocationsPerSpot,\
    minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation,\
    radiusInMiles, spotsUserGraphsFolder, graphNodesDistanceInMiles,\
    graphNodesMinEdgeWeight
from analysis import Spots, SpotsKML
from mongo_settings import locationsCollection, venuesCollection,\
    locationToLocationCollection
from library.geo import getLocationFromLid, convertMilesToRadians
from library.graphs import clusterUsingMCLClustering
import networkx as nx
from analysis.mr_analysis import locationsForUsIterator, filteredUserIterator

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
    def writeUserDistribution(): Spots.writeUserDistributionInSpots(RadiusSpots.getSpotsFile(), filteredUserIterator(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation,  fullRecord = True))
    @staticmethod
    def getStats(): return Spots.getStats(RadiusSpots.getSpotsFile(), filteredUserIterator(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation,  fullRecord = True))
    @staticmethod
    def run():
        RadiusSpots.writeAsKML()
        RadiusSpots.writeToFile()
        RadiusSpots.writeUserDistribution()
        print RadiusSpots.getStats()
        
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
#        UserGraphSpots.writeAsKML()
        UserGraphSpots.writeToFile()
        UserGraphSpots.writeUserDistribution()
        print UserGraphSpots.getStats()
if __name__ == '__main__':
    RadiusSpots.run()
#    UserGraphSpots.run()
    