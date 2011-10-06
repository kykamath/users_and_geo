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
    def write(self, fileName):
        self.kml.save(fileName)

def nearbyLocations(lid, radiusInMiles): return (location for location in locationsCollection.find({"l": {"$within": {"$center": [getLocationFromLid(lid), convertMilesToRadians(radiusInMiles)]}}}))

#def cluster():
#    i = 0
#    radiusInMiles = 5
#    graph = nx.Graph()
#    longs, lats = [], []
#    for lid in locationIterator():
#        if isWithinBoundingBox(getLocationFromLid(lid), us_boundary):
#            print i
#            for location in nearbyLocations(lid, radiusInMiles): graph.add_edge(location['_id'], lid)
#            i+=1
#            if i==50000: break
#    i = 0
#    clustersData = []
#    for component in nx.connected_components(graph):
##        if len(component)>=5: 
#        i+=1
#        longitudes, latitudes = zip(*[getLocationFromLid(l) for l in component])
#        longs+=longitudes; lats+=latitudes
##        if i==5: break;
#        clustersData.append((list(longitudes), list(latitudes), GeneralMethods.getRandomColor()))
#    print len(nx.connected_components(graph))
#    usMap = Map()
#    for lats, longs, color in clustersData: usMap.plotPoints(lats, longs, color)
#    plt.savefig('worldmap.png')

def generateRadiusSpots(radiusInMiles):
    graph = nx.Graph()
    spotsFile = radiusSpotsFolder+'%s'%(radiusInMiles)
    for lid in locationIterator():
#        if isWithinBoundingBox(getLocationFromLid(lid), us_boundary):
        for location in nearbyLocations(lid, radiusInMiles): graph.add_edge(location['_id'], lid)
    for venues in nx.connected_components(graph): FileIO.writeToFileAsJson({'venues': venues}, spotsFile)
def generateStatsForRadiusSpots(): [generateRadiusSpots(radius) for radius in [1,5,10,15,20]]
def radiusSpotsIterator(radiusInMiles, minVenues=0): return (data['venues'] for data in FileIO.iterateJsonFromFile(radiusSpotsFolder+'%s'%(radiusInMiles)) if len(data['venues'])>=minVenues)
def plotRadiusSpotDistribution():
    for radius in [1,5,10,15,20]:
        dataX, dataY = getDataDistribution((len(i) for i in radiusSpotsIterator(radius)))
        plt.loglog(dataX, dataY, label = str(radius))
        print radius, len([i for i in radiusSpotsIterator(radius, 75)] )
    plt.legend()
    plt.show()
def plotRadiusSpots(radius=10, minVenues=10):
    clustersData = []
    for venues in radiusSpotsIterator(radius, minVenues):
        longitudes, latitudes = zip(*[getLocationFromLid(l) for l in venues])
        clustersData.append((list(longitudes), list(latitudes), GeneralMethods.getRandomColor()))
    usMap = Map()
    latitudes, longitudes = [], []
    for longs, lats, color in clustersData: 
        usMap.plotPoints(lats, longs, color)
    plt.show()

def drawKMLsForRadiusSpots(radius=10, minVenues=10):
    kml = SpotsKML()
    for venues in radiusSpotsIterator(radius, minVenues): kml.addPoints(venues)
    kml.write(radiusSpotsKmlsFolder+'%s_%s.kml'%(radius, minVenues))
def generateKMLsRadiusSpots(): [generateRadiusSpots(radius) for radius in [1,5,10,15,20]]
#def generateKML(points):
#    import simplekml
#    kml = simplekml.Kml()
#    points = [list(reversed(getLocationFromLid(point))) for point in points]
#    for point in points: kml.newpoint(coords=[point])
#    pol=kml.newpolygon(outerboundaryis=geographicConvexHull(points))
#    pol.polystyle.color = '99ff00ff'  # Transparent red
#    pol.polystyle.outline = 0
#    kml.save("spots.kml")
    

    
if __name__ == '__main__':
    generateStatsForRadiusSpots()
#    plotRadiusSpotDistribution()
#    plotRadiusSpots()
#    drawKMLsForRadiusSpots()
