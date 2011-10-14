'''
Created on Oct 13, 2011

@author: kykamath
'''
import sys
sys.path.append('../')
from multiprocessing import Pool
from settings import minLocationsTheUserHasCheckedin,\
    minUniqueUsersCheckedInTheLocation, minimumLocationsPerSpot
from mongo_settings import locationsCollection
from library.geo import getLocationFromLid, convertMilesToRadians,\
    getHaversineDistanceForLids
import networkx as nx
from analysis.mr_analysis import getfilteredLocationsSet,\
    locationByUserDistributionIterator
from itertools import combinations


radiusInMiles = 4
class Spots:
    @staticmethod
    def iterateSpotsUsingRadius(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation, radiusInMiles):
        def nearbyLocations(lid, radiusInMiles): return (location for location in locationsCollection.find({"l": {"$within": {"$center": [getLocationFromLid(lid), convertMilesToRadians(radiusInMiles)]}}}))
    
        graph = nx.Graph()
        for lid in locationByUserDistributionIterator(minUniqueUsersCheckedInTheLocation):
            for location in nearbyLocations(lid, radiusInMiles): graph.add_edge(location['_id'], lid)
        for locations in nx.connected_components(graph): 
            if len(locations)>=minimumLocationsPerSpot: yield locations

if __name__ == '__main__':
    iterateSpotsUsingRadius(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation, radiusInMiles)