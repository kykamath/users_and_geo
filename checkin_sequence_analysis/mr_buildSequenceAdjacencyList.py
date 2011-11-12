'''
Created on Nov 12, 2011

@author: kykamath
'''
from library.mrjobwrapper import ModifiedMRJob
import cjson

OUTGOING_EDGE = 'out'
INCOMING_EDGE = 'in'
NODE = 'node'

class MRBuildCheckinSequenceAdjacencyList(ModifiedMRJob):
    DEFAULT_INPUT_PROTOCOL='raw_value'
    def mapper(self, key, data):
        user, [source, destination] = cjson.decode(data)
        yield source[1], [OUTGOING_EDGE, [{'u': user, 'cid': source[0], 'lid': source[1], 't': source[2]}, {'u': user, 'cid': destination[0], 'lid': destination[1], 't': destination[2]}]]
        yield destination[1], [INCOMING_EDGE, [{'u': user, 'cid': source[0], 'lid': source[1], 't': source[2]}, {'u': user, 'cid': destination[0], 'lid': destination[1], 't': destination[2]}]]
        yield source[1], [NODE, {'u': user, 'cid': source[0], 'lid': source[1], 't': source[2]}]
        yield destination[1], [NODE, {'u': user, 'cid': destination[0], 'lid': destination[1], 't': destination[2]}]
    def reducer(self, key, values): 
        def sortCheckinLists(checkins): return sorted(checkins, key=lambda c:c['t'])
        edges = {OUTGOING_EDGE: [], INCOMING_EDGE: []}
        observedCheckinIds, checkinIds = set(), []
        for type, checkinObject in values: 
            if type==NODE: 
                if checkinObject['cid'] not in observedCheckinIds: observedCheckinIds.add(checkinObject['cid']), checkinIds.append(checkinObject)
            else: edges[type].append(checkinObject)
#        for k, v in edges.items()[:]: edges[k]=sortCheckinLists(v)
        yield key, {'checkins': sortCheckinLists(checkinIds), 'edges': edges}

if __name__ == '__main__':
    MRBuildCheckinSequenceAdjacencyList.run()
