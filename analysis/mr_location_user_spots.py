'''
Created on Oct 13, 2011

@author: kykamath
'''
import sys
sys.path.append('../')
from settings import minLocationsTheUserHasCheckedin,\
    minUniqueUsersCheckedInTheLocation
from mongo_settings import locationsCollection
from library.geo import getLocationFromLid, convertMilesToRadians
import networkx as nx
from analysis.mr_analysis import getfilteredLocationsSet
    
def iterateSpotsUsingRadius(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation, radius):
    def nearbyLocations(lid, radiusInMiles): return (location for location in locationsCollection.find({"l": {"$within": {"$center": [getLocationFromLid(lid), convertMilesToRadians(radiusInMiles)]}}}))
    graph = nx.Graph()
    locationSet = getfilteredLocationsSet(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation)
    for lid in locationSet:
        for location in nearbyLocations(lid, radius): 
            if location['_id'] in locationSet: graph.add_edge(location['_id'], lid)
    for locations in nx.connected_components(graph): print locations
        
if __name__ == '__main__':
    iterateSpotsUsingRadius(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation, radius=0.5)