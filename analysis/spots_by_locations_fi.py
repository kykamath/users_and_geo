'''
Created on Oct 7, 2011

@author: kykamath
'''
import sys
sys.path.append('../')
from operator import itemgetter
from itertools import combinations
from collections import defaultdict
import networkx as nx
import matplotlib.pyplot as plt

from library.file_io import FileIO
from library.classes import GeneralMethods
from library.geo import getLocationFromLid, getHaversineDistanceForLids,\
    getLidFromLocation, getCenterOfMass, isWithinBoundingBox
from library.plotting import getDataDistribution
from library.clustering import getItemClustersFromItemsets

from analysis.mr_analysis import filteredUserIterator
from analysis import SpotsKML
from settings import locationsFIMahoutInputFile, locationsFIMahoutOutputFile,\
    initialNumberofLocationsInSpot, minSupport, userBasedSpotsKmlsFolder,\
    us_boundary
from mongo_scripts import venuesCollection

userBasedSpotsUsingFIKmlsFolder=userBasedSpotsKmlsFolder+'fi/'
maximumFIRadiusInMiles = 10 

def locationTransactionsIterator(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation):
    i = 0
    def decrementDictionary(d):
        for k in d.keys()[:]:
            d[k]-=1
            if d[k]==0: del d[k]
    
    for d in filteredUserIterator(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation):
#        while len(d.keys())>=minimumTransactionLength: 
        while d.keys(): 
            yield d.keys()
            decrementDictionary(d)
        i+=1; print i
#        if i==10: break

def locationsFromAllTransactionsIterator(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation):
    observedLocations = set()
    i=0
    for d in filteredUserIterator(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation):
        print i; i+=1
        for k in filter(lambda l: l not in observedLocations, d): observedLocations.add(k); yield getLocationFromLid(k)
#        if i==10: break;

class Mahout():
    @staticmethod
    def writeInputFileForFIMahout(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation): [FileIO.writeToFile(' '.join([i.replace(' ', '_') for i in t]), locationsFIMahoutInputFile%(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation))
                                       for t in locationTransactionsIterator(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation)]
    @staticmethod
    def calculateFrequentLocationItemsets(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation):
        inputFile = locationsFIMahoutInputFile%(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation)
        GeneralMethods.runCommand('rm -rf %s.*'%inputFile)
        GeneralMethods.runCommand('hadoop fs -rmr fi/*')
        GeneralMethods.runCommand('tar -cvf %s.tar %s'%(inputFile, inputFile))
        GeneralMethods.runCommand('gzip %s.tar'%(inputFile))
        GeneralMethods.runCommand('hadoop fs -put %s.tar.gz fi/.'%inputFile)
        GeneralMethods.runCommand('mahout fpg -i fi/mh_input_%s_%s.tar.gz -o fi/output -k 50 -g 100000 -method mapreduce -s %s'%(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation, minSupport))
    @staticmethod
    def getMahoutOutput(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation): GeneralMethods.runCommand('mahout seqdumper -s fi/output/frequentpatterns/part-r-00000 > %s'%locationsFIMahoutOutputFile%(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation, minSupport))
    @staticmethod
    def iterateFrequentLocationsFromFIMahout(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation, minCalculatedSupport, minLocationsInItemset=0, extraMinSupport=minSupport, yieldSupport=False, lids=False): 
#        for line in FileIO.iterateLinesFromFile(locationsFIMahoutOutputFile%(minUserLocations, minCalculatedSupport)):
        for line in FileIO.iterateLinesFromFile(locationsFIMahoutOutputFile%(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation, minCalculatedSupport)):
            if line.startswith('Key:'): 
                data = line.split('Value: ')[1][1:-1].split(',')
                if not lids: locationItemset, support = [getLocationFromLid(i.replace('_', ' ')) for i in data[0][1:-1].split()], int(data[1])
                else: locationItemset, support = [i.replace('_', ' ') for i in data[0][1:-1].split()], int(data[1])
                if support>=extraMinSupport and len(locationItemset)>=minLocationsInItemset: 
                    if not yieldSupport: yield [location for location in locationItemset if isWithinBoundingBox(getLocationFromLid(location), us_boundary)] 
                    else: yield [location for location in locationItemset if isWithinBoundingBox(getLocationFromLid(location), us_boundary)], support
    @staticmethod
    def analyzeFrequentLocations(minUserLocations, minCalculatedSupport):
    #    dataX, dataY = [], []
    #    for itemset, support in Mahout.iterateFrequentLocationsFromFIMahout(minUserLocations, minCalculatedSupport, yieldSupport=True): dataX.append(len(itemset)), dataY.append(support)
    #    plt.scatter(dataY, dataX)
    #    plt.title('%s'%minUserLocations), plt.ylabel('Location itemset length'); plt.xlabel('support')
    #    plt.savefig('sup_vs_itemset_length_%s.pdf'%minUserLocations)
        
    #    values = []
    #    for locations, support in Mahout.iterateFrequentLocationsFromFIMahout(minUserLocations, minCalculatedSupport, yieldSupport=True): values.append(support)
    #    dataX,dataY = getDataDistribution(values)
    #    plt.loglog(dataX, dataY)
    #    plt.title('%s'%minUserLocations), plt.ylabel('Count'); plt.xlabel('support')
    #    plt.savefig('sup_distribution_%s.pdf'%minUserLocations)
    
        values = []
        for itemset, support in Mahout.iterateFrequentLocationsFromFIMahout(minUserLocations, minCalculatedSupport, yieldSupport=True): values.append(len(itemset))
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
    
#def iterateDisjointFrequentLocationItemsets(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation, minCalculatedSupport, **kwargs):
#    observedLocations = set()
#    pointsForFuture = []
#    def locationItemsetIsDisjoint(itemset):
#        for location in itemset: 
#            if location in observedLocations: return False
#        return True 
#    for itemset, support in sorted(Mahout.iterateFrequentLocationsFromFIMahout(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation, minCalculatedSupport, yieldSupport=True, lids=True, **kwargs), key=itemgetter(1), reverse=True):
#        if locationItemsetIsDisjoint(itemset): 
#            for lid in itemset: observedLocations.add(lid)
#            yield [getLocationFromLid(lid) for lid in itemset]


def iterateDisjointFrequentLocationItemsets(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation, minCalculatedSupport, initialNumberofLocationsInSpot, **kwargs):
    observedClusters, observedLocations = {}, set()
    def splitItemSets():
        validItemSets, locationsPostponed = [], []
        for itemset, support in sorted(Mahout.iterateFrequentLocationsFromFIMahout(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation, minCalculatedSupport, yieldSupport=True, lids=True, **kwargs),
                                                          key=itemgetter(1), reverse=True):
            if len(itemset)>=initialNumberofLocationsInSpot: validItemSets.append(itemset)
            else: 
#                if len(itemset)>2: 
                locationsPostponed+=itemset
        return validItemSets, locationsPostponed
    
    def locationItemsetIsDisjoint(itemset):
        for location in itemset: 
            if location in observedLocations: return False
        return True 
    
    validItemSets, locationsPostponed = splitItemSets()
    for itemset in validItemSets:
        if locationItemsetIsDisjoint(itemset): 
            for lid in itemset: observedLocations.add(lid)
            locations = [getLocationFromLid(lid) for lid in itemset]
            observedClusters[getLidFromLocation(getCenterOfMass(locations))]=locations
        else: locationsPostponed+=itemset

    locationsPostponed = set(locationsPostponed).difference(set(observedLocations))    
    total = len(locationsPostponed)
    j=1
    for location in locationsPostponed: 
        closestItem, currentDistance = None, ()
        print total, j;j+=1
        for i in observedClusters:
            try:
                d = getHaversineDistanceForLids(i, location)
            except Exception as e: print i, location
            if currentDistance>d: 
                closestItem = i
                currentDistance=d
        if currentDistance<=50: observedClusters[closestItem].append(getLocationFromLid(location))
    return observedClusters.itervalues()

def iterateSpotsByItemsetClustering(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation, minCalculatedSupport, initialNumberofLocationsInSpot, **kwargs):
    def itemsetsIterator():
        itemsetsPostponed = []
        i=1
        for itemset, support in sorted(Mahout.iterateFrequentLocationsFromFIMahout(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation, minCalculatedSupport, yieldSupport=True, lids=True, **kwargs),
                                                          key=itemgetter(1), reverse=True):
            if len(itemset)>=initialNumberofLocationsInSpot: yield itemset
            else: itemsetsPostponed.append((itemset, len(itemset)))
#        for itemset, l in sorted(itemsetsPostponed, key=itemgetter(1), reverse=True): 
#            if l>1: yield itemset
    for cluster in getItemClustersFromItemsets(itemsetsIterator(), getHaversineDistanceForLids): 
        cluster = [getLocationFromLid(lid) for lid in cluster]
        if len(cluster)>3: 
            for c in cluster:
                print c, venuesCollection.find({'lid': c})
#            yield cluster 
            
def drawKMLsForLocationsFromAllTransactions(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation):
    SpotsKML.drawKMLsForPoints(locationsFromAllTransactionsIterator(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation), 'all_locations_%s_%s.kml'%(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation), color='#E38FF7')

#def drawKMLsForUserBasedSpotsUsingFIClusters(minSupport=minSupport, minLocations=6):
#    SpotsKML.drawKMLsForSpotsWithPoints(iterateFrequentLocationClusters(), userBasedSpotsUsingFIKmlsFolder+'fi_clusters_%s_%s.kml'%(minSupport, minLocations))
 
def drawKMLsForUserBasedOnItemsetClustering(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation, minCalculatedSupport, initialNumberofLocationsInSpot, **kwargs):
#    SpotsKML.drawKMLsForSpotsWithPoints(iterateSpotsByItemsetClustering(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation, minCalculatedSupport, initialNumberofLocationsInSpot, **kwargs), 'fi_itemset_clustering_%s_%s_%s.kml'%(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation, minCalculatedSupport))
    SpotsKML.drawKMLsForSpotsWithPoints(iterateSpotsByItemsetClustering(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation, minCalculatedSupport, initialNumberofLocationsInSpot, **kwargs), 
                                        'fi_itemset_clustering_%s_%s_%s.kml'%(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation, minCalculatedSupport),
                                        title=True)

def drawKMLsForUserBasedDisjointFrequentLocationItemsets(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation, minCalculatedSupport, **kwargs):
    SpotsKML.drawKMLsForSpotsWithPoints(iterateDisjointFrequentLocationItemsets(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation, minCalculatedSupport, **kwargs), 'fi_disjoint_%s_%s_%s.kml'%(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation, minCalculatedSupport))

    
#def drawKMLsForUserBasedSpotsUsingFI(minSupport=minSupport, minLocations=6):
#    SpotsKML.drawKMLsForSpotsWithPoints(Mahout.iterateFrequentLocationsFromFIMahout(minSupport, minLocations), userBasedSpotsUsingFIKmlsFolder+'%s_%s.kml'%(minSupport, minLocations))
    
if __name__ == '__main__':
#    Mahout.writeInputFileForFIMahout(minLocationsTheUserHasCheckedin=20, minUniqueUsersCheckedInTheLocation=10)
#    Mahout.calculateFrequentLocationItemsets(minLocationsTheUserHasCheckedin=20, minUniqueUsersCheckedInTheLocation=10)
#    Mahout.getMahoutOutput(minLocationsTheUserHasCheckedin=20, minUniqueUsersCheckedInTheLocation=10)

#    for i in [20, 50, 100, 150]: Mahout.analyzeFrequentLocations(minUserLocations=i, minCalculatedSupport=minSupport)

#    drawKMLsForLocationsFromAllTransactions(minLocationsTheUserHasCheckedin=20, minUniqueUsersCheckedInTheLocation=10)
#    drawKMLsForUserBasedDisjointFrequentLocationItemsets(minLocationsTheUserHasCheckedin=20, minUniqueUsersCheckedInTheLocation=10, minCalculatedSupport=minSupport, initialNumberofLocationsInSpot=initialNumberofLocationsInSpot) #minLocationsInItemset=10)
    drawKMLsForUserBasedOnItemsetClustering(minLocationsTheUserHasCheckedin=20, minUniqueUsersCheckedInTheLocation=10, minCalculatedSupport=minSupport, initialNumberofLocationsInSpot=initialNumberofLocationsInSpot, extraMinSupport=3)
