'''
Created on Oct 5, 2011

@author: kykamath
'''
import sys
from library.file_io import FileIO
sys.path.append('../')
from analysis import SpotsKML
from settings import userCooccurenceKmlsFolder, locationGraph
from library.geo import getLocationFromLid


locationGraphNonJsonFile=locationGraph+'NonJson'

def getLocationPairs(edge): 
    data = edge.split()
    return [data[0]+' '+data[1], data[2]+' '+data[3]]
def getNonJsonGraphFile():
    for edge in FileIO.iterateJsonFromFile(locationGraph):
        x,y=getLocationPairs(edge['e'])
        with open(locationGraphNonJsonFile, 'w') as f: 
            print '%s %s %s\n'%(x.replace(' ', '_'), y.replace(' ', '_'), edge['w'])
#            f.write('%s %s %s\n'%(x.replace(' ', '_'), y.replace(' ', '_')))

    
def drawKMLsForUserCooccurenceSpots(minEdgeWeight=30):
    kml = SpotsKML()
    i=1
    for edge in FileIO.iterateJsonFromFile(locationGraph):
        if edge['w']>=minEdgeWeight: kml.addLine(getLocationPairs(edge['e']), description=str(edge['w'])); i+=1
        if i==10000: break
    kml.write(userCooccurenceKmlsFolder+'%s.kml'%minEdgeWeight)

if __name__ == '__main__':
#    drawKMLsForUserCooccurenceSpots()
    getNonJsonGraphFile()