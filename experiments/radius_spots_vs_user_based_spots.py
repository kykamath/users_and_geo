'''
Created on Oct 7, 2011

@author: kykamath
'''
import sys
from analysis import SpotsKML
sys.path.append('../')
from analysis.mongo_scripts import locationToLocationIterator
import networkx as nx


def spotsIterator():
    def getLocationPairs(edge): 
        data = edge.split()
        return [data[0]+' '+data[1], data[2]+' '+data[3]]
    graph = nx.Graph()
    graph.add_edges_from((getLocationPairs(edge['_id']) for edge in locationToLocationIterator()))
    return (spot for spot in nx.connected_components(graph))

SpotsKML.drawKMLsForSpots(spotsIterator, 'user_based_spots.kml')

