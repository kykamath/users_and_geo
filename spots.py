'''
Created on Oct 4, 2011

@author: kykamath
'''
#import matplotlib; matplotlib.use('Agg')
from mongo_settings import locationsCollection
from library.geo import convertMilesToRadians, getLocationFromLid
#from library.graphs import plot
from collections import defaultdict
from analysis.mr_analysis import locationIterator
import matplotlib.pyplot as plt
import networkx as nx
from library.classes import GeneralMethods

radiusInMiles = 5
graph = nx.Graph()

class Map:
    @staticmethod
    def onWorldMapPlot(dataTuplesToPlot):
        from mpl_toolkits.basemap import Basemap
        m = Basemap(projection='robin',lon_0=0,resolution='c')
        for longitudes, latitudes, color in dataTuplesToPlot:
            x,y = m(longitudes,latitudes)
            m.plot(x,y,color=color, lw=0, marker='o')
#        for name,xpt,ypt in zip(cities,x,y): plt.text(xpt+50000,ypt+50000,name)
#        m.bluemarble()
        

def nearbyLocations(lid, radiusInMiles): return (location for location in locationsCollection.find({"l": {"$within": {"$center": [getLocationFromLid(lid), convertMilesToRadians(radiusInMiles)]}}}))

def cluster():
    i = 1
    for lid in locationIterator():
        print i
        for location in nearbyLocations(lid, radiusInMiles): graph.add_edge(location['_id'], lid)
        i+=1
        if i==100:
            for component in nx.connected_components(graph):
                if len(component)>=5: 
#                    graph.remove_nodes_from(component)
                    longitudes, latitudes = zip(*[getLocationFromLid(l) for l in component])
                    Map.onWorldMapPlot(longitudes, latitudes, color=GeneralMethods.getRandomColor())
#            plot(graph, node_size=20, node_color='#A0CBE2',with_labels=False)
            print len(nx.connected_components(graph))                  
            break
    plt.savefig('worldmap.pdf')
    
if __name__ == '__main__':
#    Map.onWorldMapPlot([
#                        ([-105.16, -117.16, -77.00], [40.02, 32.73, 38.55], GeneralMethods.getRandomColor()),
#                        ([-114.21, -88.10], [48.25, 17.29], GeneralMethods.getRandomColor())
#                        ])
#    plt.show()
#    cluster()

    from mpl_toolkits.basemap import Basemap
    import numpy as np
    import matplotlib.pyplot as plt
    # llcrnrlat,llcrnrlon,urcrnrlat,urcrnrlon
    # are the lat/lon values of the lower left and upper right corners
    # of the map.
    # resolution = 'i' means use intermediate resolution coastlines.
    # lon_0, lat_0 are the central longitude and latitude of the projection.
#    m = Basemap(llcrnrlon=-10.5,llcrnrlat=49.5,urcrnrlon=3.5,urcrnrlat=59.5,
#                resolution='i',projection='tmerc',lon_0=-4.36,lat_0=54.7)
    m = Basemap(projection='robin',lon_0=0,resolution='c')
    # can get the identical map this way (by specifying width and
    # height instead of lat/lon corners)
    #m = Basemap(width=894887,height=1116766,\
    #            resolution='i',projection='tmerc',lon_0=-4.36,lat_0=54.7)
    m.drawcoastlines()
    m.fillcontinents(color='#FFFFFF',lake_color='#FFFFFF')
    # draw parallels and meridians.
#    m.drawparallels(np.arange(-40,61.,2.))
#    m.drawmeridians(np.arange(-20.,21.,2.))
    m.drawmapboundary(fill_color='#FFFFFF') 
    plt.title("Transverse Mercator Projection")
    plt.savefig('map.png')