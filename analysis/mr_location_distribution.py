'''
Created on Oct 3, 2011

@author: kykamath
'''
from library.mrjobwrapper import ModifiedMRJob
from library.geo import parseData, getLidFromLocation

class MRLocationDistribution(ModifiedMRJob):
    DEFAULT_INPUT_PROTOCOL='raw_value'
    def mapper(self, key, line):
        data = parseData(line)
        if data: yield getLidFromLocation(data['l']), 1
    def reducer(self, location, occurrences): yield location, {'location': location, 'count':sum(occurrences)}

if __name__ == '__main__':
    MRLocationDistribution.run()
