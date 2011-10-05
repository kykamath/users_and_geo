'''
Created on Oct 4, 2011

@author: kykamath
'''
import matplotlib
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
matplotlib.use('Agg')

class Map:
    @staticmethod
    def onWorldMapPlot(longitudes, latitudes, color='#C704C4'):
        from mpl_toolkits.basemap import Basemap
        m = Basemap(projection='robin',lon_0=0,resolution='c')
        x,y = m(longitudes,latitudes)
        m.plot(x,y,color=color, lw=0, marker='o')
#        for name,xpt,ypt in zip(cities,x,y): plt.text(xpt+50000,ypt+50000,name)
        m.bluemarble()
        

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
#    Map.onWorldMapPlot([-105.16, -117.16, -77.00], [40.02, 32.73, 38.55])
#    Map.onWorldMapPlot([-114.21, -88.10], [48.25, 17.29], color=GeneralMethods.getRandomColor())
#    plt.show()
    cluster()