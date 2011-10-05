'''
Created on Oct 4, 2011

@author: kykamath
'''
#import matplotlib; matplotlib.use('Agg')
from mongo_settings import locationsCollection
from library.geo import convertMilesToRadians, getLocationFromLid, isWithinBoundingBox
#from library.graphs import plot
from collections import defaultdict
from analysis.mr_analysis import locationIterator
import matplotlib.pyplot as plt
import networkx as nx
from library.classes import GeneralMethods
from settings import us_boundary
from library.plotting import Map

radiusInMiles = 5
graph = nx.Graph()

#class Map:
#    @staticmethod
#    def onWorldMapPlot(dataTuplesToPlot):
#        from mpl_toolkits.basemap import Basemap
#        m = Basemap(projection='robin',lon_0=0,resolution='c')
#        m.drawcoastlines()
#        m.fillcontinents(color='#FFFFFF',lake_color='#FFFFFF')
#        m.drawmapboundary(fill_color='#FFFFFF') 
#        for longitudes, latitudes, color in dataTuplesToPlot:
#            print longitudes, latitudes, color
#            x,y = m(longitudes,latitudes)
#            m.plot(x,y,color=color, lw=0, marker='.')
##        for name,xpt,ypt in zip(cities,x,y): plt.text(xpt+50000,ypt+50000,name)
##        m.bluemarble()
#
#    @staticmethod
#    def onUSMapPlot(dataTuplesToPlot):
#        from mpl_toolkits.basemap import Basemap
##        m = Basemap(projection='robin',lon_0=0,resolution='c')
#        minLat, minLon, maxLat, maxLon = [item for t in us_boundary for item in t]
#        m = Basemap(llcrnrlon=minLon, llcrnrlat=minLat, urcrnrlon=maxLon, urcrnrlat = maxLat,  resolution = 'h', projection = 'merc', lon_0 = minLon+(maxLon-minLon)/2, lat_0 = minLat+(maxLat-minLat)/2)
#        
#        # Region definition (Agulhas) minLon = 16
#        maxLon = 35
#        maxLat = -20
#        minLat = -43
#        # Base Map set-up with Mercator projection
#        
#        m = Basemap(llcrnrlon=minLon, llcrnrlat=minLat, urcrnrlon=maxLon, urcrnrlat = maxLat, resolution = 'h', projection = 'merc', \
#        lon_0 = minLon+(maxLon-minLon)/2, \
#        lat_0 = minLat+(maxLat-minLat)/2)
#        
#        m.drawcoastlines()
#        m.fillcontinents(color='#FFFFFF',lake_color='#FFFFFF')
#        m.drawmapboundary(fill_color='#FFFFFF') 
#        for longitudes, latitudes, color in dataTuplesToPlot:
#            print longitudes, latitudes, color
#            x,y = m(longitudes,latitudes)
#            m.plot(x,y,color=color, lw=0, marker='.')
##        for name,xpt,ypt in zip(cities,x,y): plt.text(xpt+50000,ypt+50000,name)
##        m.bluemarble()

        

def nearbyLocations(lid, radiusInMiles): return (location for location in locationsCollection.find({"l": {"$within": {"$center": [getLocationFromLid(lid), convertMilesToRadians(radiusInMiles)]}}}))

def cluster():
    i = 0
    longs, lats = [], []
    for lid in locationIterator():
        if isWithinBoundingBox(getLocationFromLid(lid), us_boundary):
            print i
            for location in nearbyLocations(lid, radiusInMiles): graph.add_edge(location['_id'], lid)
            i+=1
#            if i==100000000:
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
    for lats, longs, color in clustersData: usMap.plotPoints()
    plt.savefig('worldmap.png')
    
    
if __name__ == '__main__':
#    Map.onUSMapPlot([
#                        ([-105.16, -117.16, -77.00], [40.02, 32.73, 38.55], GeneralMethods.getRandomColor()),
#                        ([-114.21, -88.10], [48.25, 17.29], GeneralMethods.getRandomColor())
#                        ])
##    plt.savefig('map.png')
#    plt.show()

#    Map.onWorldMapPlot([
#          ([-0.025000000000000001, -0.024, -0.024, -0.024, -0.025000000000000001, -0.023, -0.027, -0.025999999999999999, -0.025999999999999999], [109.33799999999999, 109.337, 109.339, 109.33799999999999, 109.337, 109.337, 109.339, 109.339, 109.33799999999999], '#bd8668'),
#          ([-0.029000000000000001, -0.028000000000000001, -0.028000000000000001, -0.027, -0.027], [109.34099999999999, 109.342, 109.34099999999999, 109.34099999999999, 109.342], '#ff0000'),
#          ([-114.21, -88.10], [48.25, 17.29], GeneralMethods.getRandomColor()),
#     ])
#    plt.savefig('map.png')

    cluster()
#    temp()