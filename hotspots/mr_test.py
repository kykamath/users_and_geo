'''
Created on Nov 16, 2011

@author: kykamath
'''
from library.mrjobwrapper import ModifiedMRJob
from library.geo import parseData, getLidFromLocation, isWithinBoundingBox, getLatticeLid
import cjson

#boundary = [[24.527135,-127.792969], [49.61071,-59.765625]] # US
boundary = [[40.491, -74.356], [41.181, -72.612]] # NY

class MRTest(ModifiedMRJob):
    DEFAULT_INPUT_PROTOCOL='raw_value'
    def mapper(self, key, line):
#        data = parseData(line)
        yield cjson.decode(line), 1
    def reducer(self, key, _): yield 1, key

if __name__ == '__main__':
    MRTest.run()