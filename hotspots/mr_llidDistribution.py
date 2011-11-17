'''
Created on Nov 16, 2011

@author: kykamath
'''
from library.mrjobwrapper import ModifiedMRJob
from library.geo import parseData, getLidFromLocation, isWithinBoundingBox, getLatticeLid
import cjson

#boundary = [[24.527135,-127.792969], [49.61071,-59.765625]] # US
#boundary = [[40.491, -74.356], [41.181, -72.612]] # NY

class MRLlidDistribution(ModifiedMRJob):
    DEFAULT_INPUT_PROTOCOL='raw_value'
    def mapper(self, key, line):
        data = cjson.decode(line)
        yield getLatticeLid(data['geo'], accuracy=0.001), 1
    def reducer(self, key, values): yield key, {'llid': key, 'checkins': sum([v for v in values])}

if __name__ == '__main__':
    MRLlidDistribution.run()