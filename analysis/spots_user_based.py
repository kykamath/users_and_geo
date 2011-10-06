'''
Created on Oct 5, 2011

@author: kykamath
'''
import sys, os
sys.path.append('../')
from analysis import SpotsKML
from settings import userCooccurenceKmlsFolder, locationGraph,\
    userBasedSpotsFolder
from library.geo import getLocationFromLid
from library.file_io import FileIO

locationGraphNonJsonFile=locationGraph+'NonJson'
userBasedSpotsUsingClusteringFolder=userBasedSpotsFolder+'clustering/'

def getLocationPairs(edge): 
    data = edge.split()
    return [data[0]+' '+data[1], data[2]+' '+data[3]]
def getNonJsonGraphFile():
    f = open(locationGraphNonJsonFile, 'w') 
    for edge in FileIO.iterateJsonFromFile(locationGraph):
        x,y=getLocationPairs(edge['e'])
        f.write('%s %s %s\n'%(x.replace(' ', '_'), y.replace(' ', '_'), edge['w']))
    f.close()
def clusterNonJsonGraphFile(inflation):
    spotsFile = userBasedSpotsUsingClusteringFolder+str(inflation)
    temporaryFile = '/tmp/fname.out'
    os.system('mcl %s -I %s --abc -o %s'%(locationGraphNonJsonFile, inflation, temporaryFile))
    for line in open(temporaryFile): FileIO.writeToFileAsJson({'locations': [i.replace('_', ' ') for i in line.strip().split()]}, spotsFile)
    os.system('rm -rf %s'%temporaryFile)
    
def drawKMLsForUserCooccurenceSpots(minEdgeWeight=30):
    kml = SpotsKML()
    i=1
    for edge in FileIO.iterateJsonFromFile(locationGraph):
        if edge['w']>=minEdgeWeight: kml.addLine(getLocationPairs(edge['e']), description=str(edge['w'])); i+=1
        if i==10000: break
    kml.write(userCooccurenceKmlsFolder+'%s.kml'%minEdgeWeight)

if __name__ == '__main__':
#    drawKMLsForUserCooccurenceSpots()
#    getNonJsonGraphFile()
    clusterNonJsonGraphFile(15)