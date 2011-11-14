'''
Created on Nov 11, 2011

@author: kykamath
'''
import sys, time
sys.path.append('../')
from library.classes import GeneralMethods
from library.file_io import FileIO
from mongo_settings import checkinsCollection,\
    checkinSequenceLocationsCollection
from analysis.mr_analysis import filteredUserIterator
from settings import minLocationsTheUserHasCheckedin,\
    minUniqueUsersCheckedInTheLocation, checkinSequenceGraphFile,\
    checkinSequenceLocationRegexFolder
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
    @staticmethod
    def nPreviousLocations(checkin, users, numberOfCheckins=1, **kwargs):
        userCheckins = [c[0] for c in users[str(checkin['u'])]]
        index = userCheckins.index(checkin['cid'])
        if index>=numberOfCheckins: return users[str(checkin['u'])][index-numberOfCheckins:index]
        else: return users[str(checkin['u'])][:index]
    @staticmethod
    def nFutureLocations(checkin, users, numberOfCheckins=1, **kwargs):
        userCheckins = [c[0] for c in users[str(checkin['u'])]]
        index = userCheckins.index(checkin['cid'])
        return users[str(checkin['u'])][index+1:index+numberOfCheckins+1]
#    @staticmethod
#    def nCheckinsInFuture(checkin, users, numberOfCheckins=1, **kwargs):
#        userCheckins = [c[0] for c in users[str(checkin['u'])]]
#        index = userCheckins.index(checkin['cid'])
#        return users[str(checkin['u'])][:index+numberOfCheckins]
class NeighboringClusters():
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
    def getNeigboringLocationClusters(inputLocationObject, neighborLocationsSelectionMethod, **kwargs):
        neighborLocations = [neighborLocationsSelectionMethod(checkin, inputLocationObject['users'], **kwargs) for checkin in inputLocationObject['checkins']]
        ''' From these neighbor location we have to select every (location, checkinid) pair only 
        once to build the neigbor relation graph, that should be clustered. 
            We should also remove checkins that done at the current location
        '''
        neighborLocationCheckins = NeighboringClusters._filterCheckins(neighborLocations, inputLocationObject['lid'])
        return NeighboringClusters.getClustersUsingGraph(neighborLocationCheckins, **kwargs)
    @staticmethod
    def getClustersUsingGraph(checkins, minEdgeWeight = 0, **kwargs):
        graph = nx.Graph()
        for cIns in checkins:
            [updateNode(c[1], graph) for c in cIns]
            if len(cIns)>=2: [updateEdge(u[1], v[1], graph) for u, v in combinations(cIns, 2)]
        for u, v in graph.edges()[:]:
            if graph.edge[u][v]['w']<minEdgeWeight: graph.remove_edge(u, v) 
        return sorted([(lids, nodeScores(lids, graph)) for lids in nx.connected_components(graph)], key=itemgetter(1), reverse=True)
    @staticmethod
    def getNeigboringLocationClustersForRegex(regex, neighborLocationExtractionMethod, **kwargs):
        inputFileName = checkinSequenceLocationRegexFolder+regex
        for data in FileIO.iterateJsonFromFile(inputFileName):
            clusters = NeighboringClusters.getNeigboringLocationClusters(data, neighborLocationExtractionMethod, **kwargs)
            for cluster in clusters:
                print cluster
            exit()

if __name__ == '__main__':
#    writeCheckinSequenceGraphFile()
#    createLocationFile(regex='cafe')
    
    NeighboringClusters.getNeigboringLocationClustersForRegex('cafe', NeighborLocationsSelection.nPreviousLocations, minEdgeWeight=3, numberOfCheckins=5)

#    NeighboringClusters.getNeigboringLocationClusters('cafe')
#    NeighboringClusters.getNeigboringLocationClusters('cafe', OUTGOING_EDGE)
