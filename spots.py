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
#        m = Basemap(projection='robin',lon_0=0,resolution='c')
        m = Basemap(projection='robin',lon_0=0,resolution='c')
        m.drawcoastlines()
        m.fillcontinents(color='#FFFFFF',lake_color='#FFFFFF')
        m.drawmapboundary(fill_color='#FFFFFF') 
        for longitudes, latitudes, color in dataTuplesToPlot:
            print longitudes, latitudes, color
            x,y = m(longitudes,latitudes)
            m.plot(x,y,color=color, lw=0, marker='o')
#        for name,xpt,ypt in zip(cities,x,y): plt.text(xpt+50000,ypt+50000,name)
#        m.bluemarble()
        

def nearbyLocations(lid, radiusInMiles): return (location for location in locationsCollection.find({"l": {"$within": {"$center": [getLocationFromLid(lid), convertMilesToRadians(radiusInMiles)]}}}))

def cluster():
    i = 1
    clustersData = []
    for lid in locationIterator():
        print i
        for location in nearbyLocations(lid, radiusInMiles): graph.add_edge(location['_id'], lid)
        i+=1
        if i==100000:
            for component in nx.connected_components(graph):
                if len(component)>=5: 
#                    graph.remove_nodes_from(component)
                    longitudes, latitudes = zip(*[getLocationFromLid(l) for l in component])
                    clustersData.append((list(longitudes), list(latitudes), GeneralMethods.getRandomColor()))
#            plot(graph, node_size=20, node_color='#A0CBE2',with_labels=False)
            print len(nx.connected_components(graph))
            break
    print len(clustersData)
    Map.onWorldMapPlot(clustersData)
    plt.savefig('worldmap.pdf')
    
def temp():
    from mpl_toolkits.basemap import Basemap
    import matplotlib.pyplot as plt
    import numpy as np
    
    # lon_0 is central longitude of robinson projection.
    # resolution = 'c' means use crude resolution coastlines.
    m = Basemap(projection='robin',lon_0=0,resolution='c')
    #set a background colour
    m.drawmapboundary(fill_color='#85A6D9')
    
    # draw coastlines, country boundaries, fill continents.
    m.fillcontinents(color='white',lake_color='#85A6D9')
    m.drawcoastlines(color='#6D5F47', linewidth=.4)
    m.drawcountries(color='#6D5F47', linewidth=.4)
    
    # draw lat/lon grid lines every 30 degrees.
    m.drawmeridians(np.arange(-180, 180, 30), color='#bbbbbb')
    m.drawparallels(np.arange(-90, 90, 30), color='#bbbbbb')

    # lat/lon coordinates of top ten world cities
    lats = [35.69,37.569,19.433,40.809,18.975,-6.175,-23.55,28.61,34.694,31.2]
    lngs = [139.692,126.977,-99.133,-74.02,72.825,106.828,-46.633,77.23,135.502,121.5]
    populations = [32.45,20.55,20.45,19.75,19.2,18.9,18.85,18.6,17.375,16.65] #millions

    # compute the native map projection coordinates for cities
    x,y = m(lngs,lats)

    #scale populations to emphasise different relative pop sizes
    s_populations = [p * p for p in populations]

    #scatter scaled circles at the city locations
    m.scatter(
        x,
        y,
        s=s_populations, #size
        c='blue', #color
        marker='o', #symbol
        alpha=0.25, #transparency
        zorder = 2, #plotting order
        )
    
    # plot population labels of the ten cities.
    for population, xpt, ypt in zip(populations, x, y):
        label_txt = int(round(population, 0)) #round to 0 dp and display as integer
        plt.text(
            xpt,
            ypt,
            label_txt,
            color = 'blue',
            size='small',
            horizontalalignment='center',
            verticalalignment='center',
            zorder = 3,
            )

    #add a title and display the map on screen
    plt.title('Top Ten World Metropolitan Areas By Population')
    plt.savefig('map.png')



    
if __name__ == '__main__':
#    Map.onWorldMapPlot([
#                        ([-105.16, -117.16, -77.00], [40.02, 32.73, 38.55], GeneralMethods.getRandomColor()),
#                        ([-114.21, -88.10], [48.25, 17.29], GeneralMethods.getRandomColor())
#                        ])
#    plt.savefig('map.png')

#    Map.onWorldMapPlot([
#          ([-0.025000000000000001, -0.024, -0.024, -0.024, -0.025000000000000001, -0.023, -0.027, -0.025999999999999999, -0.025999999999999999], [109.33799999999999, 109.337, 109.339, 109.33799999999999, 109.337, 109.337, 109.339, 109.339, 109.33799999999999], '#bd8668'),
#          ([-0.029000000000000001, -0.028000000000000001, -0.028000000000000001, -0.027, -0.027], [109.34099999999999, 109.342, 109.34099999999999, 109.34099999999999, 109.342], '#ff0000'),
#          ([-114.21, -88.10], [48.25, 17.29], GeneralMethods.getRandomColor()),
#     ])
#    plt.savefig('map.png')

#    cluster()
    temp()