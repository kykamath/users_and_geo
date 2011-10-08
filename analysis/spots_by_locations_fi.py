'''
Created on Oct 7, 2011

@author: kykamath
'''
import sys
sys.path.append('../')
from analysis.mr_analysis import userToLocationMapIterator
from library.file_io import FileIO
from library.classes import GeneralMethods
from settings import locationsFIMahoutInputFile, locationsFIMahoutOutputFile,\
    minimumTransactionLength, minSupport, userBasedSpotsKmlsFolder
from analysis import SpotsKML
from library.geo import getLocationFromLid, getHaversineDistanceForLids
import matplotlib.pyplot as plt
from library.plotting import getDataDistribution
from itertools import combinations
import networkx as nx

userBasedSpotsUsingFIKmlsFolder=userBasedSpotsKmlsFolder+'fi/'
maximumFIRadiusInMiles = 10 

def locationTransactionsIterator():
    i = 0
    def decrementDictionary(d):
        for k in d.keys()[:]:
            d[k]-=1
            if d[k]==0: del d[k]
    
    for d in userToLocationMapIterator(minLocations=150):
        while len(d.keys())>=minimumTransactionLength: 
            yield d.keys()
            decrementDictionary(d)
        i+=1
        print i
#        if i==10: break

def writeInputFileForFIMahout(): [FileIO.writeToFile(' '.join([i.replace(' ', '_') for i in t]), locationsFIMahoutInputFile) for t in locationTransactionsIterator()]

def calculateFrequentLocationItemsets():
    GeneralMethods.runCommand('rm -rf %s.*'%locationsFIMahoutInputFile)
    GeneralMethods.runCommand('hadoop fs -rmr fi/*')
    GeneralMethods.runCommand('tar -cvf %s.tar %s'%(locationsFIMahoutInputFile, locationsFIMahoutInputFile))
    GeneralMethods.runCommand('gzip %s.tar'%(locationsFIMahoutInputFile))
    GeneralMethods.runCommand('hadoop fs -put %s.tar.gz fi/.'%locationsFIMahoutInputFile)
    GeneralMethods.runCommand('mahout fpg -i fi/mh_input.tar.gz -o fi/output -k 50 -method mapreduce -s %s'%minSupport)
def getMahoutOutput(): GeneralMethods.runCommand('mahout seqdumper -s fi/output/frequentpatterns/part-r-00000 > %s'%locationsFIMahoutOutputFile)
    
def iterateFrequentLocationsFromFIMahout(minSupport=minSupport, minLocations=6, yieldSupport=False, lids=False): 
    for line in FileIO.iterateLinesFromFile(locationsFIMahoutOutputFile):
        if line.startswith('Key:'): 
            data = line.split('Value: ')[1][1:-1].split(',')
            if not lids: locationItemset, support = [getLocationFromLid(i.replace('_', ' ')) for i in data[0][1:-1].split()], int(data[1])
            else: locationItemset, support = [i.replace('_', ' ') for i in data[0][1:-1].split()], int(data[1])
            if support>minSupport and len(locationItemset)>=minLocations: 
                if not yieldSupport: yield locationItemset 
                else: yield locationItemset, support

def analyzeFrequentLocations():
#    dataX, dataY = [], []
#    for locations, support in iterateFrequentLocationsFromFIMahout(minSupport=0, minLocations=0, yieldSupport=True): dataX.append(len(locations)), dataY.append(support)
#    plt.scatter(dataX, dataY)
#    plt.xlabel('# of locations'); plt.ylabel('support')
    values = []
    for locations, support in iterateFrequentLocationsFromFIMahout(minSupport=0, minLocations=0, yieldSupport=True): values.append(support)
    dataX,dataY = getDataDistribution(values)
    plt.loglog(dataX, dataY)
    plt.show()
    
def iterateFrequentLocationClusters():
    graph = nx.Graph()
    for locations, support in iterateFrequentLocationsFromFIMahout(minSupport=0, minLocations=0, yieldSupport=True, lids=True):
        for l1, l2 in combinations(locations,2): 
            if l2 not in graph or l1 not in graph or l1 not in graph[l2] and getHaversineDistanceForLids(l1, l2)<=maximumFIRadiusInMiles: graph.add_edge(l1,l2)
    for cluster in nx.connected_components(graph): yield [getLocationFromLid(lid) for lid in cluster]
    
    
def drawKMLsForUserBasedSpotsUsingFIClusters(minSupport=minSupport, minLocations=6):
    SpotsKML.drawKMLsForSpotsWithPoints(iterateFrequentLocationClusters(), userBasedSpotsUsingFIKmlsFolder+'fi_clusters_%s_%s.kml'%(minSupport, minLocations))
    
def drawKMLsForUserBasedSpotsUsingFI(minSupport=minSupport, minLocations=6):
    SpotsKML.drawKMLsForSpotsWithPoints(iterateFrequentLocationsFromFIMahout(minSupport, minLocations), userBasedSpotsUsingFIKmlsFolder+'%s_%s.kml'%(minSupport, minLocations))
    
    
if __name__ == '__main__':
#    writeInputFileForFIMahout()
    calculateFrequentLocationItemsets()
#    getMahoutOutput()
#    drawKMLsForUserBasedSpotsUsingFI(minSupport=10)
#    analyzeFrequentLocations()
#    drawKMLsForUserBasedSpotsUsingFIClusters()