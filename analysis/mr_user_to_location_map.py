'''
Created on Oct 3, 2011

@author: kykamath
'''
from library.mrjobwrapper import ModifiedMRJob
from itertools import groupby
from library.geo import parseData, getLidFromLocation

class MRUserToLocationMap(ModifiedMRJob):
    DEFAULT_INPUT_PROTOCOL='raw_value'
    def mapper(self, key, line):
        data = parseData(line)
        if data: yield data['u'], getLidFromLocation(data['l'])
    def reducer(self, word, occurrences):
        occurences = [str(s) for s in occurrences]
        if len(occurences)>10:  yield word, dict([(k, len(list(g))) for k, g in groupby(sorted(occurences))])

if __name__ == '__main__':
    MRUserToLocationMap.run()