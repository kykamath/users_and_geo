'''
Created on Oct 7, 2011

@author: kykamath
'''
import sys
sys.path.append('../')
from operator import itemgetter
from itertools import combinations
import networkx as nx
import matplotlib.pyplot as plt

from library.file_io import FileIO
from library.classes import GeneralMethods
from library.geo import getLocationFromLid, getHaversineDistanceForLids
from library.plotting import getDataDistribution

from analysis.mr_analysis import userToLocationMapIterator
from analysis import SpotsKML
from settings import locationsFIMahoutInputFile, locationsFIMahoutOutputFile,\
    minimumTransactionLength, minSupport, userBasedSpotsKmlsFolder

userBasedSpotsUsingFIKmlsFolder=userBasedSpotsKmlsFolder+'fi/'
maximumFIRadiusInMiles = 10 

def locationTransactionsIterator(minLocations):
    i = 0
    def decrementDictionary(d):
        for k in d.keys()[:]:
            d[k]-=1
            if d[k]==0: del d[k]
    
    for d in userToLocationMapIterator(minLocations=minLocations):
        while len(d.keys())>=minimumTransactionLength: 
            yield d.keys()
            decrementDictionary(d)
        i+=1
        print i
#        if i==10: break

def locationsFromAllTransactionsIterator(minLocations):
    observedLocations = set()
    for d in userToLocationMapIterator(minLocations=minLocations):
        for k in filter(lambda l: l not in observedLocations, d): observedLocations.add(k); yield k

class Mahout():
    @staticmethod
    def writeInputFileForFIMahout(): [FileIO.writeToFile(' '.join([i.replace(' ', '_') for i in t]), locationsFIMahoutInputFile) for t in locationTransactionsIterator()]
    @staticmethod
    def calculateFrequentLocationItemsets():
        GeneralMethods.runCommand('rm -rf %s.*'%locationsFIMahoutInputFile)
        GeneralMethods.runCommand('hadoop fs -rmr fi/*')
        GeneralMethods.runCommand('tar -cvf %s.tar %s'%(locationsFIMahoutInputFile, locationsFIMahoutInputFile))
        GeneralMethods.runCommand('gzip %s.tar'%(locationsFIMahoutInputFile))
        GeneralMethods.runCommand('hadoop fs -put %s.tar.gz fi/.'%locationsFIMahoutInputFile)
        GeneralMethods.runCommand('mahout fpg -i fi/mh_input.tar.gz -o fi/output -k 50 -g 100000 -method mapreduce -s %s'%minSupport)
    @staticmethod
    def getMahoutOutput(minUserLocations, minSupport): GeneralMethods.runCommand('mahout seqdumper -s fi/output/frequentpatterns/part-r-00000 > %s'%locationsFIMahoutOutputFile%(minUserLocations, minSupport))
    @staticmethod
    def iterateFrequentLocationsFromFIMahout(minUserLocations, minCaluclatedSupport, minLocationsInItemset=0, extraMinSupport=minSupport, yieldSupport=False, lids=False): 
        for line in FileIO.iterateLinesFromFile(locationsFIMahoutOutputFile%(minUserLocations, minCaluclatedSupport)):
            if line.startswith('Key:'): 
                data = line.split('Value: ')[1][1:-1].split(',')
                if not lids: locationItemset, support = [getLocationFromLid(i.replace('_', ' ')) for i in data[0][1:-1].split()], int(data[1])
                else: locationItemset, support = [i.replace('_', ' ') for i in data[0][1:-1].split()], int(data[1])
                if support>=extraMinSupport and len(locationItemset)>=minLocationsInItemset: 
                    if not yieldSupport: yield locationItemset 
                    else: yield locationItemset, support
    @staticmethod
    def analyzeFrequentLocations(minUserLocations, minCaluclatedSupport):
    #    dataX, dataY = [], []
    #    for itemset, support in Mahout.iterateFrequentLocationsFromFIMahout(minUserLocations, minCaluclatedSupport, yieldSupport=True): dataX.append(len(itemset)), dataY.append(support)
    #    plt.scatter(dataY, dataX)
    #    plt.title('%s'%minUserLocations), plt.ylabel('Location itemset length'); plt.xlabel('support')
    #    plt.savefig('sup_vs_itemset_length_%s.pdf'%minUserLocations)
        
    #    values = []
    #    for locations, support in Mahout.iterateFrequentLocationsFromFIMahout(minUserLocations, minCaluclatedSupport, yieldSupport=True): values.append(support)
    #    dataX,dataY = getDataDistribution(values)
    #    plt.loglog(dataX, dataY)
    #    plt.title('%s'%minUserLocations), plt.ylabel('Count'); plt.xlabel('support')
    #    plt.savefig('sup_distribution_%s.pdf'%minUserLocations)
    
        values = []
        for itemset, support in Mahout.iterateFrequentLocationsFromFIMahout(minUserLocations, minCaluclatedSupport, yieldSupport=True): values.append(len(itemset))
        dataX,dataY = getDataDistribution(values)
        plt.loglog(dataX, dataY)
        plt.title('%s'%minUserLocations), plt.ylabel('Count'); plt.xlabel('Location itemset length')
        plt.savefig('location_itemsets_distribution_%s.pdf'%minUserLocations)

def iterateFrequentLocationClusters():
    graph = nx.Graph()
    for locations, support in Mahout.iterateFrequentLocationsFromFIMahout(minSupport=0, minLocations=0, yieldSupport=True, lids=True):
        for l1, l2 in combinations(locations,2): 
            if l2 not in graph or l1 not in graph or l1 not in graph[l2] and getHaversineDistanceForLids(l1, l2)<=maximumFIRadiusInMiles: graph.add_edge(l1,l2)
    for cluster in nx.connected_components(graph): yield [getLocationFromLid(lid) for lid in cluster]
    
def iterateDisjointFrequentLocationItemsets(minUserLocations, minCaluclatedSupport, **kwargs):
    observedLocations = set()
    def locationItemsetIsDisjoint(itemset):
        for location in itemset: 
            if location in observedLocations: return False
        return True 
    for itemset, support in sorted(Mahout.iterateFrequentLocationsFromFIMahout(minUserLocations, minCaluclatedSupport, yieldSupport=True, lids=True, **kwargs), key=itemgetter(1), reverse=True):
        if locationItemsetIsDisjoint(itemset): 
            for lid in itemset: observedLocations.add(lid)
            yield [getLocationFromLid(lid) for lid in itemset]
            

def drawKMLsForLocationsFromAllTransactions(minUserLocations):
    SpotsKML.drawKMLsForPoints(locationsFromAllTransactionsIterator(minUserLocations), 'all_locations_%s.kml'%(minUserLocations), color='#E38FF7')

#def drawKMLsForUserBasedSpotsUsingFIClusters(minSupport=minSupport, minLocations=6):
#    SpotsKML.drawKMLsForSpotsWithPoints(iterateFrequentLocationClusters(), userBasedSpotsUsingFIKmlsFolder+'fi_clusters_%s_%s.kml'%(minSupport, minLocations))
    
def drawKMLsForUserBasedDisjointFrequentLocationItemsets(minUserLocations, minCaluclatedSupport, **kwargs):
    SpotsKML.drawKMLsForSpotsWithPoints(iterateDisjointFrequentLocationItemsets(minUserLocations, minCaluclatedSupport, **kwargs), 'fi_disjoint_%s_%s.kml'%(minUserLocations, minCaluclatedSupport))
    
#def drawKMLsForUserBasedSpotsUsingFI(minSupport=minSupport, minLocations=6):
#    SpotsKML.drawKMLsForSpotsWithPoints(Mahout.iterateFrequentLocationsFromFIMahout(minSupport, minLocations), userBasedSpotsUsingFIKmlsFolder+'%s_%s.kml'%(minSupport, minLocations))
    
    
if __name__ == '__main__':
#    Mahout.writeInputFileForFIMahout()
#    Mahout.calculateFrequentLocationItemsets()
#    Mahout.getMahoutOutput(minUserLocations=20, minSupport=minSupport)
#    drawKMLsForUserBasedSpotsUsingFI(minSupport=10)
#    for i in [20, 50, 100, 150]: Mahout.analyzeFrequentLocations(minUserLocations=i, minCaluclatedSupport=minSupport)
#    drawKMLsForUserBasedSpotsUsingFIClusters()
#    drawKMLsForUserBasedDisjointFrequentLocationItemsets(minUserLocations=20, minCaluclatedSupport=minSupport, extraMinSupport=minSupport, minLocationsInItemset=10)
#    drawKMLsForLocationsFromAllTransactions(minUserLocations=20)
    
    print len(locationsFromAllTransactionsIterator(minLocations=20))