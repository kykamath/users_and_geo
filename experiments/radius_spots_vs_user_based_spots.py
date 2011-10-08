'''
Created on Oct 7, 2011

@author: kykamath
'''
import sys
sys.path.append('../')
from settings import expMinimumLocationsPerSpot
from library.geo import getLocationFromLid
from analysis import SpotsKML
from analysis.mongo_scripts import locationToLocationIterator
import networkx as nx


def spotsIterator():
    def getLocationPairs(edge): 
        data = edge.split()
        return [data[0]+' '+data[1], data[2]+' '+data[3]]
    graph = nx.Graph()
    graph.add_edges_from((getLocationPairs(edge['_id']) for edge in locationToLocationIterator()))
    return ((getLocationFromLid(s) for s in spot) for spot in nx.connected_components(graph) if len(spot)>=expMinimumLocationsPerSpot)

SpotsKML.drawKMLsForSpotsWithPoints(spotsIterator(), 'user_based_spots.kml')
