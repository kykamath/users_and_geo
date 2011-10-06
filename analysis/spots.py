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
from library.geo import geographicConvexHull
from settings import radiusSpotsFolder, radiusSpotsKmlsFolder
from library.plotting import Map, getDataDistribution

class SpotsKML:
    def __init__(self):
        import simplekml
        self.kml = simplekml.Kml()
    def addPoints(self, points, color=None):
        if not color: color=GeneralMethods.getRandomColor()
        points = [list(reversed(getLocationFromLid(point))) for point in points]
#        for point in points: self.kml.newpoint(coords=[point])
        self.kml.newpoint(coords=[points[0]])
        pol=self.kml.newpolygon(outerboundaryis=geographicConvexHull(points))
        pol.polystyle.color = '99'+color[1:]  # Transparent red
        pol.polystyle.outline = 0
    def write(self, fileName): self.kml.save(fileName)

def nearbyLocations(lid, radiusInMiles): return (location for location in locationsCollection.find({"l": {"$within": {"$center": [getLocationFromLid(lid), convertMilesToRadians(radiusInMiles)]}}}))

def generateRadiusSpots(radiusInMiles):
    graph = nx.Graph()
    spotsFile = radiusSpotsFolder+'%s'%(radiusInMiles)
    print 'Creating:', spotsFile
    for lid in locationIterator():
#        if isWithinBoundingBox(getLocationFromLid(lid), us_boundary):
        for location in nearbyLocations(lid, radiusInMiles): graph.add_edge(location['_id'], lid)
    for locations in nx.connected_components(graph): FileIO.writeToFileAsJson({'venues': locations}, spotsFile)
def generateStatsForRadiusSpots(): [generateRadiusSpots(radius) for radius in [1,5,10,15,20]]
def radiusSpotsIterator(radiusInMiles, minLocations=0): return (data['venues'] for data in FileIO.iterateJsonFromFile(radiusSpotsFolder+'%s'%(radiusInMiles)) if len(data['venues'])>=minLocations)
def plotRadiusSpotDistribution():
    for radius in [1,5,10,15,20]:
        dataX, dataY = getDataDistribution((len(i) for i in radiusSpotsIterator(radius)))
        plt.loglog(dataX, dataY, label = str(radius))
        print radius, len([i for i in radiusSpotsIterator(radius, 75)] )
    plt.legend()
    plt.show()
def plotRadiusSpots(radius=10, minLocations=10):
    clustersData = []
    for locations in radiusSpotsIterator(radius, minLocations):
        longitudes, latitudes = zip(*[getLocationFromLid(l) for l in locations])
        clustersData.append((list(longitudes), list(latitudes), GeneralMethods.getRandomColor()))
    usMap = Map()
    latitudes, longitudes = [], []
    for longs, lats, color in clustersData: 
        usMap.plotPoints(lats, longs, color)
    plt.show()

def drawKMLsForRadiusSpots(radius=10, minLocations=10):
    kml = SpotsKML()
    for locations in radiusSpotsIterator(radius, minLocations): kml.addPoints(locations)
    kml.write(radiusSpotsKmlsFolder+'%s_%s.kml'%(radius, minLocations))
def generateKMLsRadiusSpots(): [generateRadiusSpots(radius) for radius in [1,5,10,15,20]]
    
if __name__ == '__main__':
    generateStatsForRadiusSpots()
#    plotRadiusSpotDistribution()
#    plotRadiusSpots()
#    drawKMLsForRadiusSpots()
