'''
Created on Oct 3, 2011

@author: kykamath
'''
from library.mrjobwrapper import ModifiedMRJob
from library.geo import parseData

class MRUserDistribution(ModifiedMRJob):
    DEFAULT_INPUT_PROTOCOL='raw_value'
    def mapper(self, key, line):
        data = parseData(line)
        if data: yield data['u'], 1
    def reducer(self, user, occurrences): yield user, {'user': user, 'count':sum(occurrences)}

if __name__ == '__main__':
    MRUserDistribution.run()
