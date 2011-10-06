'''
Created on Oct 5, 2011

@author: kykamath
'''
import sys
sys.path.append('../')
from analysis import SpotsKML
from settings import userCooccurenceKmlsFolder
from library.geo import getLocationFromLid
import cjson

def getLocationPairs(edge): 
    data = edge.split()
    return [getLocationFromLid(data[0]+' '+data[1]), getLocationFromLid(data[2]+' '+data[3])]
    
    
def drawKMLsForUserCooccurenceSpots():
    kml = SpotsKML()
#    for locations in radiusSpotsIterator(radius, minLocations): kml.addPoints(locations)
    for edge in [cjson.decode('{"e": "63.299 28.050 63.290 28.025", "w": 1}')]:
        print edge
#        print getLocationPairs(edge['e'])
        kml.addLine(getLocationPairs(edge['e']), description=str(edge['w']))
#        kml.addLine(getLocationPairs(edge['e']))
    kml.write(userCooccurenceKmlsFolder+'user_co_occurence.kml')

if __name__ == '__main__':
    drawKMLsForUserCooccurenceSpots()