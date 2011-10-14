'''
Created on Oct 13, 2011

@author: kykamath
'''
import sys
sys.path.append('../')
from settings import spotsRadiusFolder, minimumLocationsPerSpot,\
    minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation,\
    radiusInMiles
from analysis import Spots, SpotsKML
from mongo_settings import locationsCollection, venuesCollection
from library.geo import getLocationFromLid, convertMilesToRadians
import networkx as nx
from analysis.mr_analysis import locationsForUsIterator

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
    def iterateSpotsUsingRadius(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation, radiusInMiles):
        def nearbyLocations(lid, radiusInMiles): return (location for location in locationsCollection.find({"l": {"$within": {"$center": [getLocationFromLid(lid), convertMilesToRadians(radiusInMiles)]}}}))
        graph = nx.Graph()
        for lid in locationsForUsIterator(minUniqueUsersCheckedInTheLocation):
            for location in nearbyLocations(lid, radiusInMiles): graph.add_edge(location['_id'], lid)
        for locations in nx.connected_components(graph): 
            if len(locations)>=minimumLocationsPerSpot: yield getKMLForCluster(locations)
    @staticmethod
    def writeToFile(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation, radiusInMiles):
        spotsFile = '%s/%s_%s_%s'%(spotsRadiusFolder, minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation, radiusInMiles)
        Spots.writeSpotsToFile(RadiusSpots.iterateSpotsUsingRadius(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation, radiusInMiles), spotsFile)
    @staticmethod
    def writeAsKML(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation, radiusInMiles):
        spotsFile = '%s/%s_%s_%s'%(spotsRadiusFolder, minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation, radiusInMiles)
        SpotsKML.drawKMLsForSpotsWithPoints(RadiusSpots.iterateSpotsUsingRadius(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation, radiusInMiles), 
                                            '%s.kml'%(spotsFile), title=True)
    @staticmethod
    def run():
        RadiusSpots.writeAsKML(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation, radiusInMiles)
#        RadiusSpots.writeToFile(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation, radiusInMiles)

if __name__ == '__main__':
    RadiusSpots.run()
    