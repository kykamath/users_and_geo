'''
Created on Oct 3, 2011

@author: kykamath

Build venue to venue graph, 
with edge weight equal to the 
number of common users.
'''
from library.mrjobwrapper import ModifiedMRJob
from library.geo import parseData, getLidFromLocation
from itertools import combinations

class MRLocationGraphByUsers(ModifiedMRJob):
    DEFAULT_INPUT_PROTOCOL='raw_value'
    def collectLocationsMapper(self, key, line):
        data = parseData(line)
        if data: yield data['u'], getLidFromLocation(data['l'])
    def collectLocationsReducer(self, user, locations): yield user, {'user': user, 'locations':list(set(list(locations)))}
    
    def buildGraphMapper(self, key, data):
        if len(data['locations'])>=2:
            for v in combinations(data['locations'], 2): yield '%s %s'%v, 1
    def buildGraphReducer(self, edge, occurrences): 
        cooccurence = sum(occurrences)
        yield edge, {'e': edge, 'w':cooccurence}
    
    def steps(self):
        return [
                self.mr(self.collectLocationsMapper, self.collectLocationsReducer),
                self.mr(self.buildGraphMapper, self.buildGraphReducer),
                ]

if __name__ == '__main__':
    MRLocationGraphByUsers.run()
