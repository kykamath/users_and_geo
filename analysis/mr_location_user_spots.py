'''
Created on Oct 13, 2011

@author: kykamath
Why not geo?
=> Does not work in dense areas. New York?
=> Doesn't discover regions related but apart? Like discover xxx|yyy|zz instead of xxx|yyy|xx 

'''
import sys
sys.path.append('../')
from library.file_io import FileIO
from library.plotting import getDataDistribution
from analysis.spots_by_locations_fi import locationTransactionsIterator
from library.classes import GeneralMethods
from settings import spotsRadiusFolder, minimumLocationsPerSpot,\
    minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation,\
    radiusInMiles, spotsUserGraphsFolder, graphNodesDistanceInMiles,\
    graphNodesMinEdgeWeight, spotsFrequentItemsFolder,\
    locationsFIMahoutInputFile, locationsFIMahoutOutputFile, minSupport,\
    us_boundary
from analysis import Spots, SpotsKML
from mongo_settings import locationsCollection, venuesCollection,\
    locationToLocationCollection
from library.geo import getLocationFromLid, convertMilesToRadians,\
    isWithinBoundingBox
from library.graphs import clusterUsingMCLClustering
import networkx as nx
from analysis.mr_analysis import locationsForUsIterator, filteredUserIterator
import matplotlib.pyplot as plt
from operator import itemgetter

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
                    if not yieldSupport: yield [location for location in locationItemset if isWithinBoundingBox(location, us_boundary)] 
                    else: yield [location for location in locationItemset if isWithinBoundingBox(getLocationFromLid(location), us_boundary)], support
    @staticmethod
    def iterateLocations(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation, *args, **kwargs):
        for locations in Mahout.iterateFrequentLocationsFromFIMahout(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation, *args, **kwargs):
            for location in locations: yield location
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

def getKMLForCluster(cluster):
    clusterToYield = []
    if len(cluster)>3: 
        for lid in cluster:
            title = venuesCollection.find_one({'lid':lid})
            if title!=None: clusterToYield.append((getLocationFromLid(lid), unicode(title['n']).encode("utf-8")))
            else: clusterToYield.append((getLocationFromLid(lid), ''))
    return clusterToYield

class RadiusSpots:
    @staticmethod
    def getSpotsFile(): return '%s/%s_%s_%s'%(spotsRadiusFolder, minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation, radiusInMiles)
    @staticmethod
    def iterateSpots():
        def nearbyLocations(lid, radiusInMiles): return (location for location in locationsCollection.find({"l": {"$within": {"$center": [getLocationFromLid(lid), convertMilesToRadians(radiusInMiles)]}}}))
        graph = nx.Graph()
        for lid in locationsForUsIterator(minUniqueUsersCheckedInTheLocation):
            for location in nearbyLocations(lid, radiusInMiles): graph.add_edge(location['_id'], lid)
        for locations in nx.connected_components(graph): 
            if len(locations)>=minimumLocationsPerSpot: yield getKMLForCluster(locations)
    @staticmethod
    def writeToFile(): Spots.writeSpotsToFile(RadiusSpots.iterateSpots(), RadiusSpots.getSpotsFile())
    @staticmethod
    def writeAsKML(): SpotsKML.drawKMLsForSpotsWithPoints(RadiusSpots.iterateSpots(), '%s.kml'%(RadiusSpots.getSpotsFile()), title=True)
    @staticmethod
    def writeUserDistribution(): Spots.writeUserDistributionInSpots(RadiusSpots.getSpotsFile(), filteredUserIterator(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation,  fullRecord = True))
    @staticmethod
    def getStats(): return Spots.getStats(RadiusSpots.getSpotsFile(), filteredUserIterator(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation,  fullRecord = True))
    @staticmethod
    def run():
        RadiusSpots.writeAsKML()
        RadiusSpots.writeToFile()
        RadiusSpots.writeUserDistribution()
        print RadiusSpots.getStats()
        
class UserGraphSpots:
    @staticmethod
    def getSpotsFile(): return '%s/%s_%s_%s'%(spotsUserGraphsFolder, minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation, graphNodesDistanceInMiles)
    @staticmethod
    def iterateSpots():
        locationsToCheck = set(list(locationsForUsIterator(minUniqueUsersCheckedInTheLocation)))
        graph = nx.Graph()
        for e in locationToLocationCollection.find():
            d = e['_id'].split()
            l1, l2 = ' '.join(d[:2]), ' '.join(d[2:])
            if l1 in locationsToCheck and l2 in locationsToCheck and e['d']<=graphNodesDistanceInMiles: graph.add_edge(l1.replace(' ', '_'), l2.replace(' ', '_'), {'w': e['u']})
        for locations in nx.connected_components(graph): 
            if len(locations)>=minimumLocationsPerSpot: 
                clusters = clusterUsingMCLClustering(graph.subgraph(locations), inflation=20)
                print graph.subgraph(locations).number_of_nodes(), graph.subgraph(locations).number_of_edges(), len(clusters)
                for cluster in clusters: 
                    if len(cluster)>=minimumLocationsPerSpot:  yield getKMLForCluster([c.replace('_', ' ') for c in cluster])
    @staticmethod
    def writeToFile(): Spots.writeSpotsToFile(UserGraphSpots.iterateSpots(), UserGraphSpots.getSpotsFile())
    @staticmethod
    def writeAsKML(): SpotsKML.drawKMLsForSpotsWithPoints(UserGraphSpots.iterateSpots(), '%s.kml'%(UserGraphSpots.getSpotsFile()), title=True)
    @staticmethod
    def writeUserDistribution(): Spots.writeUserDistributionInSpots(UserGraphSpots.getSpotsFile(), filteredUserIterator(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation,  fullRecord = True))
    @staticmethod
    def getStats(): return Spots.getStats(UserGraphSpots.getSpotsFile(), filteredUserIterator(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation,  fullRecord = True))
    @staticmethod
    def run():
        UserGraphSpots.writeAsKML()
        UserGraphSpots.writeToFile()
        UserGraphSpots.writeUserDistribution()
        print UserGraphSpots.getStats()

class FrequentItemSpots:        
    @staticmethod
    def getSpotsFile(): return '%s/%s_%s'%(spotsFrequentItemsFolder, minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation)
    @staticmethod
    def iterateSpots():
        itemsetsPostponed = []
        for itemset, support in sorted(Mahout.iterateFrequentLocationsFromFIMahout(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation, minSupport, yieldSupport=True, lids=True), key=itemgetter(1), reverse=True):
            if itemset: print support, itemset
        exit()
#            if len(itemset)>=initialNumberofLocationsInSpot: yield itemset
#            else: itemsetsPostponed.append((itemset, len(itemset)))
#        for itemset, l in sorted(itemsetsPostponed, key=itemgetter(1), reverse=True): 
#            if l>1: yield itemset
#    for cluster in getItemClustersFromItemsets(itemsetsIterator(), getHaversineDistanceForLids): 
#        if len(cluster)>minimumLocationsPerSpot: yield getClusterForKML(cluster)
    @staticmethod
    def writeAsKML(): SpotsKML.drawKMLsForSpotsWithPoints(FrequentItemSpots.iterateSpots(), '%s.kml'%(FrequentItemSpots.getSpotsFile()), title=True)
    @staticmethod
    def run():
        FrequentItemSpots.writeAsKML()
        
if __name__ == '__main__':
#    RadiusSpots.run()
#    UserGraphSpots.run()
    FrequentItemSpots.run()