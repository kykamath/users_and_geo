'''
Created on Oct 4, 2011

@author: kykamath
'''
import sys
from library.file_io import FileIO
sys.path.append('../')
from mongo_settings import locationsCollection
from library.geo import convertMilesToRadians, getLocationFromLid, isWithinBoundingBox
from analysis.mr_analysis import locationIterator
import matplotlib.pyplot as plt
import networkx as nx
from library.classes import GeneralMethods
from settings import us_boundary, radiusSpotsFolder
from library.plotting import Map

def nearbyLocations(lid, radiusInMiles): return (location for location in locationsCollection.find({"l": {"$within": {"$center": [getLocationFromLid(lid), convertMilesToRadians(radiusInMiles)]}}}))

def cluster():
    i = 0
    radiusInMiles = 5
    graph = nx.Graph()
    longs, lats = [], []
    for lid in locationIterator():
        if isWithinBoundingBox(getLocationFromLid(lid), us_boundary):
            print i
            for location in nearbyLocations(lid, radiusInMiles): graph.add_edge(location['_id'], lid)
            i+=1
            if i==50000: break
    i = 0
    clustersData = []
    for component in nx.connected_components(graph):
#        if len(component)>=5: 
        i+=1
        longitudes, latitudes = zip(*[getLocationFromLid(l) for l in component])
        longs+=longitudes; lats+=latitudes
#        if i==5: break;
        clustersData.append((list(longitudes), list(latitudes), GeneralMethods.getRandomColor()))
    print len(nx.connected_components(graph))
    usMap = Map()
    for lats, longs, color in clustersData: usMap.plotPoints(lats, longs, color)
    plt.savefig('worldmap.png')

def generateRadiusSpots(radiusInMiles):
    graph = nx.Graph()
    spotsFile = radiusSpotsFolder+'%s'%(radiusInMiles)
    FileIO.writeToFileAsJson({'radius_in_miles': radiusInMiles}, spotsFile)
    i = 0
    for lid in locationIterator():
        if isWithinBoundingBox(getLocationFromLid(lid), us_boundary):
            for location in nearbyLocations(lid, radiusInMiles): graph.add_edge(location['_id'], lid)
            i+=1
            if i==100: break
    for venues in nx.connected_components(graph): FileIO.writeToFileAsJson({'venues': venues}, spotsFile)
        
def generateStatsForRadiusSpots():
    for radius in [1,5,10,15,20]: generateRadiusSpots(radius)
            
    
if __name__ == '__main__':
    generateStatsForRadiusSpots()
