'''
Created on Oct 7, 2011

@author: kykamath
'''
import sys
sys.path.append('../')
from analysis.mr_analysis import userToLocationMapIterator
from library.file_io import FileIO
from operator import itemgetter
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
    
    for d in userToLocationMapIterator(minLocations=5):
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
    GeneralMethods.runCommand('mahout fpg -i fi/mh_input.tar.gz -o fi/output -k 50 -g 100000 -method mapreduce -s %s'%minSupport)
def getMahoutOutput(minUserLocations, minSupport): GeneralMethods.runCommand('mahout seqdumper -s fi/output/frequentpatterns/part-r-00000 > %s'%locationsFIMahoutOutputFile%(minUserLocations, minSupport))
    
def iterateFrequentLocationsFromFIMahout(minUserLocations, minCaluclatedSupport, minLocationsInItemset=0, extraMinSupport=minSupport, yieldSupport=False, lids=False): 
    for line in FileIO.iterateLinesFromFile(locationsFIMahoutOutputFile%(minUserLocations, minCaluclatedSupport)):
        if line.startswith('Key:'): 
            data = line.split('Value: ')[1][1:-1].split(',')
            if not lids: locationItemset, support = [getLocationFromLid(i.replace('_', ' ')) for i in data[0][1:-1].split()], int(data[1])
            else: locationItemset, support = [i.replace('_', ' ') for i in data[0][1:-1].split()], int(data[1])
            if support>extraMinSupport and len(locationItemset)>=minLocationsInItemset: 
                if not yieldSupport: yield locationItemset 
                else: yield locationItemset, support

def analyzeFrequentLocations(minUserLocations, minCaluclatedSupport):
#    dataX, dataY = [], []
#    for itemset, support in iterateFrequentLocationsFromFIMahout(minUserLocations, minCaluclatedSupport, yieldSupport=True): dataX.append(len(itemset)), dataY.append(support)
#    plt.scatter(dataY, dataX)
#    plt.title('%s'%minUserLocations), plt.ylabel('Location itemset length'); plt.xlabel('support')
#    plt.savefig('sup_vs_itemset_length_%s.pdf'%minUserLocations)
    
#    values = []
#    for locations, support in iterateFrequentLocationsFromFIMahout(minUserLocations, minCaluclatedSupport, yieldSupport=True): values.append(support)
#    dataX,dataY = getDataDistribution(values)
#    plt.loglog(dataX, dataY)
#    plt.title('%s'%minUserLocations), plt.ylabel('Count'); plt.xlabel('support')
#    plt.savefig('sup_distribution_%s.pdf'%minUserLocations)

    values = []
    for itemset, support in iterateFrequentLocationsFromFIMahout(minUserLocations, minCaluclatedSupport, yieldSupport=True): values.append(len(itemset))
    dataX,dataY = getDataDistribution(values)
    plt.loglog(dataX, dataY)
    plt.title('%s'%minUserLocations), plt.ylabel('Count'); plt.xlabel('Location itemset length')
    plt.savefig('location_itemsets_distribution_%s.pdf'%minUserLocations)

#    plt.show()

def iterateFrequentLocationClusters():
    graph = nx.Graph()
    for locations, support in iterateFrequentLocationsFromFIMahout(minSupport=0, minLocations=0, yieldSupport=True, lids=True):
        for l1, l2 in combinations(locations,2): 
            if l2 not in graph or l1 not in graph or l1 not in graph[l2] and getHaversineDistanceForLids(l1, l2)<=maximumFIRadiusInMiles: graph.add_edge(l1,l2)
    for cluster in nx.connected_components(graph): yield [getLocationFromLid(lid) for lid in cluster]
    
def getDisjointFrequentLocationItemsets(minUserLocations, minCaluclatedSupport):
    for itemset, support in sorted(iterateFrequentLocationsFromFIMahout(minUserLocations, minCaluclatedSupport, yieldSupport=True), key=itemgetter(1), reverse=True):
        print itemset, support
    
def drawKMLsForUserBasedSpotsUsingFIClusters(minSupport=minSupport, minLocations=6):
    SpotsKML.drawKMLsForSpotsWithPoints(iterateFrequentLocationClusters(), userBasedSpotsUsingFIKmlsFolder+'fi_clusters_%s_%s.kml'%(minSupport, minLocations))
    
def drawKMLsForUserBasedSpotsUsingFI(minSupport=minSupport, minLocations=6):
    SpotsKML.drawKMLsForSpotsWithPoints(iterateFrequentLocationsFromFIMahout(minSupport, minLocations), userBasedSpotsUsingFIKmlsFolder+'%s_%s.kml'%(minSupport, minLocations))
    
    
if __name__ == '__main__':
#    writeInputFileForFIMahout()
#    calculateFrequentLocationItemsets()
#    getMahoutOutput(minUserLocations=20, minSupport=minSupport)
#    drawKMLsForUserBasedSpotsUsingFI(minSupport=10)
#    for i in [20, 50, 100, 150]: analyzeFrequentLocations(minUserLocations=i, minCaluclatedSupport=minSupport)
#    drawKMLsForUserBasedSpotsUsingFIClusters()
    getDisjointFrequentLocationItemsets(minUserLocations=20, minCaluclatedSupport=minSupport)
    