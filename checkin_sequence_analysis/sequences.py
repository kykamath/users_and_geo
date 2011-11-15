'''
Created on Nov 11, 2011

@author: kykamath
'''
import sys, time
sys.path.append('../')
from library.geo import isWithinBoundingBox, getLocationFromLid
from library.classes import GeneralMethods
from library.file_io import FileIO
from mongo_settings import checkinsCollection,\
    checkinSequenceLocationsCollection, venuesCollection
from analysis.mr_analysis import filteredUserIterator
from settings import minLocationsTheUserHasCheckedin,\
    minUniqueUsersCheckedInTheLocation, checkinSequenceGraphFile,\
    checkinSequenceLocationRegexFolder,\
    checkinSequenceLocationRegexAnalysisFolder, us_boundary
from operator import itemgetter
from itertools import combinations
import networkx as nx

OUTGOING_EDGE = 'out'
INCOMING_EDGE = 'in'

def updateNode(node, graph, w=1): 
    if not graph.has_node(node): graph.add_node(node, {'w':0})
    if 'w' not in graph.node[node]: graph.node[node]['w']=0
    graph.node[node]['w']+=w
def updateEdge(u,v, graph, w=1):
    if not graph.has_edge(u, v): graph.add_edge(u,v, {'w':0})
    graph.edge[u][v]['w']+=1
def nodeScores(nodes, graph): return sum(graph.node[node]['w'] for node in nodes)

def writeCheckinSequenceGraphFile():   
    userSet = set([userVector['user'] for userVector in filteredUserIterator(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation, fullRecord = True)])
    count, total = 1, len(userSet)
    for user in userSet:
        print user, count, total
        checkins = [(c['_id'], c['lid'], time.mktime(c['t'].timetuple())) for c in checkinsCollection.find({'u': user})]
        for i in GeneralMethods.getElementsInWindow(checkins, 2): FileIO.writeToFileAsJson([user, i], checkinSequenceGraphFile)
        count+=1

def createLocationFile(regex):
    def getCheckinsForUser(user): return [[checkin['_id'], checkin['lid'], time.mktime(checkin['t'].timetuple())] for checkin in checkinsCollection.find({'u':user})]
    fileName = checkinSequenceLocationRegexFolder+regex
    for location in checkinSequenceLocationsCollection.find({'n':{'$regex':regex}}):
        userCheckins = {}
        print location['_id'], location['n']
        users = set([edge[0]['u'] for type in [OUTGOING_EDGE, INCOMING_EDGE] for edge in location['e'][type]])
        for user in users: 
            if user not in userCheckins: userCheckins[str(user)] = getCheckinsForUser(user)
        location['users'] = userCheckins
        location['edges'] = location['e']; del location['e']
        location['checkins'] = location['c']; del location['c']
        location['lid'] = location['_id']; del location['_id']
        location['name'] = location['n']; del location['n']
        FileIO.writeToFileAsJson(location, fileName)

class NeighborLocationsSelection:
    ''' This class should return a history of user checkins correponding to a particular user.
    For example: given a user and his checkin at starbucks, this method shoould return his previous 5 checkins
    before this.
    '''
    N_PREVIOUS_LOCATIONS = 'nPreviousLocations'
    N_FUTURE_LOCATIONS = 'nFutureLocations'
    N_LOCATIONS = 'nLocations'
    @staticmethod
    def nPreviousLocations(checkin, users, checkinsWindow=1, **kwargs):
        userCheckins = [c[0] for c in users[str(checkin['u'])]]
        index = userCheckins.index(checkin['cid'])
        if index>=checkinsWindow: return users[str(checkin['u'])][index-checkinsWindow:index]
        else: return users[str(checkin['u'])][:index]
    @staticmethod
    def nFutureLocations(checkin, users, checkinsWindow=1, **kwargs):
        userCheckins = [c[0] for c in users[str(checkin['u'])]]
        index = userCheckins.index(checkin['cid'])
        return users[str(checkin['u'])][index+1:index+checkinsWindow+1]
    @staticmethod
    def nLocations(checkin, users, checkinsWindow=1, **kwargs):
        return NeighborLocationsSelection.nPreviousLocations(checkin, users, checkinsWindow, **kwargs) + NeighborLocationsSelection.nFutureLocations(checkin, users, checkinsWindow, **kwargs)
    @staticmethod
    def getMethod(id):
        return {
                      NeighborLocationsSelection.N_PREVIOUS_LOCATIONS: NeighborLocationsSelection.nPreviousLocations,
                      NeighborLocationsSelection.N_FUTURE_LOCATIONS: NeighborLocationsSelection.nFutureLocations,
                      NeighborLocationsSelection.N_LOCATIONS: NeighborLocationsSelection.nLocations
                  }[id]
class NeighboringLocationsAnalysis():
    ''' Neighbor locations clusters  are the clusters of locations that come to a particular place or
    go from a particular place. For example: starbucks get clusters of people from IT and moms.
    
    Neigbor Relation Graph:  A graph with locations as vertices and edges showing that same user visited,
    both the locations. Clustering this graph will give us classes of different users.
    '''
    @staticmethod
    def _filterCheckins(checkins, lid):
        ''' 
            This method preforms the following actions:
                i) Removes duplicateCheckins
                ii) Removes checkins that are from a lid (input parameter)
            checkin = [checkinid, lid, time]
            checkins = [[checkin1, checkin2], [checkin1, checkin2, checkin3]]
        '''
        observedCheckins, checkinsToReturn = set(), []
        for cIns in checkins:
            cInsToReturn  = []
            for c in cIns:
                if c[0] not in observedCheckins and c[1]!=lid: observedCheckins.add(c[0]), cInsToReturn.append(c)
            if cInsToReturn: checkinsToReturn.append(cInsToReturn)
        return checkinsToReturn
    @staticmethod
    def analyzeLocation(inputLocationObject, neighborLocationsSelectionMethod, **kwargs):
        neighborLocations = [neighborLocationsSelectionMethod(checkin, inputLocationObject['users'], **kwargs) for checkin in inputLocationObject['checkins']]
        ''' From these neighbor location we have to select every (location, checkinid) pair only 
        once to build the neigbor relation graph, that should be clustered. 
            We should also remove checkins that done at the current location
        '''
        analysis = {}
        neighborLocationCheckins = NeighboringLocationsAnalysis._filterCheckins(neighborLocations, inputLocationObject['lid'])
        graph = NeighboringLocationsAnalysis.getNeigboringLocationGraph(neighborLocationCheckins, **kwargs)
        analysis['clusters']=NeighboringLocationsAnalysis.getClustersFromGraph(graph, **kwargs)
        kwargs['minEdgeWeightInNRGraph'] = ()
        analysis['neigboringLocations']=NeighboringLocationsAnalysis.getClustersFromGraph(graph, **kwargs)
        return analysis
    @staticmethod
    def getClustersFromGraph(graph, minEdgeWeightInNRGraph = 0, **kwargs):
        for u, v in graph.edges()[:]:
            if graph.edge[u][v]['w']<minEdgeWeightInNRGraph: graph.remove_edge(u, v) 
        return sorted([(lids, nodeScores(lids, graph)) for lids in nx.connected_components(graph)], key=itemgetter(1), reverse=True)
    @staticmethod
    def getNeigboringLocationGraph(checkins, **kwargs):
        graph = nx.Graph()
        for cIns in checkins:
            [updateNode(c[1], graph) for c in cIns]
            if len(cIns)>=2: [updateEdge(u[1], v[1], graph) for u, v in combinations(cIns, 2)]
        return graph
    @staticmethod
    def analyze(regex, neighborLocationExtractionMethod, **kwargs):
        def getListWithLocationNames(locationList):
            listToReturn = []
            for cluster, score in locationList:
                clusterNames = []
                for c in cluster:
                    object = venuesCollection.find_one({'lid': c})
                    if object: clusterNames.append((object['n'], c))
                    else: clusterNames.append(('', c))
                listToReturn.append((clusterNames, score))
            return listToReturn
        inputFileName = checkinSequenceLocationRegexFolder+regex
        outputFileName = checkinSequenceLocationRegexAnalysisFolder+neighborLocationExtractionMethod+'/'+regex
        analyzedData = {'parameters': kwargs, 'locations': {}}
        for data in FileIO.iterateJsonFromFile(inputFileName):
            print 'Analyzing:', kwargs['checkinsWindow'], data['lid']
            analysis = NeighboringLocationsAnalysis.analyzeLocation(data, NeighborLocationsSelection.getMethod(neighborLocationExtractionMethod), **kwargs)
            analysis['neigboringLocations'] = getListWithLocationNames(analysis['neigboringLocations'])
            analysis['clusters'] = getListWithLocationNames(analysis['clusters'])
            analyzedData['locations'][data['lid']] = analysis
        FileIO.writeToFileAsJson(analyzedData, outputFileName)
    @staticmethod
    def generateData():
        for i in range(1,11):
            regex = 'starbuck'
#            NeighboringLocationsAnalysis.analyze(regex, NeighborLocationsSelection.N_PREVIOUS_LOCATIONS, minEdgeWeightInNRGraph=3, checkinsWindow=i)
#            NeighboringLocationsAnalysis.analyze(regex, NeighborLocationsSelection.N_FUTURE_LOCATIONS, minEdgeWeightInNRGraph=3, checkinsWindow=i)
            NeighboringLocationsAnalysis.analyze(regex, NeighborLocationsSelection.N_LOCATIONS, minEdgeWeightInNRGraph=10, checkinsWindow=i)
    @staticmethod
    def analyzeData():
#        regex = 'mcdonald'
        regex = 'cafe'
        neighborLocationExtractionMethod = NeighborLocationsSelection.N_LOCATIONS
        inputFile = checkinSequenceLocationRegexAnalysisFolder+neighborLocationExtractionMethod+'/'+regex
        for line in FileIO.iterateJsonFromFile(inputFile):
#            for location, data in line['locations'].iteritems():
            data = line['locations']['41.895 -87.623']
            print line['parameters']['checkinsWindow'], [l[0][0] for l, _ in data['neigboringLocations'][:5]]
#                if isWithinBoundingBox(getLocationFromLid(location), us_boundary):
#                    print line['parameters']['checkinsWindow'], location, [l[0][0] for l, _ in data['neigboringLocations'][:5]]
#            exit()

    @staticmethod
    def analyzeDataClusters():
        regex = 'cafe'
        neighborLocationExtractionMethod = NeighborLocationsSelection.N_LOCATIONS
        inputFile = checkinSequenceLocationRegexAnalysisFolder+neighborLocationExtractionMethod+'/'+regex
        for line in FileIO.iterateJsonFromFile(inputFile):
            if line['parameters']['checkinsWindow']==10:
                for location, data in line['locations'].iteritems():
    #                data = line['locations']['41.895 -87.623']
                    if isWithinBoundingBox(getLocationFromLid(location), us_boundary):
                        print venuesCollection.find_one({'lid': location})['n'], location,'\n'
                        for l, _ in data['clusters'][:5]:
                            print [i[0] for i in l]
                        print '\n ********** \n'
#            exit()


class NeigboringLocationsGraph:
    @staticmethod
    def writeGraphs(regex, neighborLocationExtractionMethod, **kwargs):
        def getLocationName(lid):
            object = venuesCollection.find_one({'lid': lid})
            if object: return object['n']
            else: return lid
        inputFileName = checkinSequenceLocationRegexFolder+regex
        outputFileName = checkinSequenceLocationRegexAnalysisFolder+neighborLocationExtractionMethod+'/'+regex
        for data in FileIO.iterateJsonFromFile(inputFileName):
            outputFileName = checkinSequenceLocationRegexAnalysisFolder+neighborLocationExtractionMethod+'/graph/'+regex+'/'+data['lid']+'_%s'%kwargs['checkinsWindow']
            print 'Analyzing:', kwargs['checkinsWindow'], data['lid'], outputFileName
            graph = NeigboringLocationsGraph.getLocationGraph(data,  NeighborLocationsSelection.getMethod(neighborLocationExtractionMethod), **kwargs)
            edges = []
            for u, v in graph.edges_iter(): edges.append([getLocationName(u), getLocationName(v)])
            for u,v in edges: FileIO.writeToFile('%s\t%s'%(unicode(u).encode('utf-8'),unicode(v).encode('utf-8')), outputFileName)
#            analysis = NeighboringLocationsAnalysis.analyzeLocation(data, NeighborLocationsSelection.getMethod(neighborLocationExtractionMethod), **kwargs)
    @staticmethod
    def getLocationGraph(inputLocationObject, neighborLocationsSelectionMethod, **kwargs):
        neighborLocations = [neighborLocationsSelectionMethod(checkin, inputLocationObject['users'], **kwargs) for checkin in inputLocationObject['checkins']]
        analysis = {}
        neighborLocationCheckins = NeighboringLocationsAnalysis._filterCheckins(neighborLocations, inputLocationObject['lid'])
        return NeighboringLocationsAnalysis.getNeigboringLocationGraph(neighborLocationCheckins, **kwargs)
    @staticmethod
    def generateData():
        regex = 'starbuck'
        NeigboringLocationsGraph.writeGraphs(regex, NeighborLocationsSelection.N_LOCATIONS, checkinsWindow=5)

if __name__ == '__main__':
#    writeCheckinSequenceGraphFile()
#    createLocationFile(regex='starbuck')
    
#    NeighboringLocationsAnalysis.generateData()
#    NeighboringLocationsAnalysis.analyzeDataClusters()
    NeigboringLocationsGraph.generateData()
    

