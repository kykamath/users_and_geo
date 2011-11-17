'''
Created on Nov 16, 2011

@author: kykamath
'''
from library.mrjobwrapper import ModifiedMRJob
from library.geo import parseData, getLidFromLocation, isWithinBoundingBox, getLatticeLid
import time

#boundary = [[24.527135,-127.792969], [49.61071,-59.765625]] # US
boundary = [[40.491, -74.356], [41.181, -72.612]] # NY

class MRCheckinsByBoundary(ModifiedMRJob):
    DEFAULT_INPUT_PROTOCOL='raw_value'
    def mapper(self, key, line):
        data = parseData(line)
        if data and isWithinBoundingBox(data['l'], boundary): 
            del data['_id']
            data['t'] = time.mktime(data['t'].timetuple())
            data['lid'] = getLidFromLocation(data['l'])
            data['llid'] = getLatticeLid(data['l'], accuracy=0.015)
            yield data, 1
    def reducer(self, key, _): yield 1, key

if __name__ == '__main__':
    MRCheckinsByBoundary.run()