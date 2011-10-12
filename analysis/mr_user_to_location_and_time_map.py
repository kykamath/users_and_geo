'''
Created on Oct 3, 2011

@author: kykamath
'''
from library.mrjobwrapper import ModifiedMRJob
from library.geo import parseData, getLidFromLocation
from collections import defaultdict

class MRUserToLocationAndTimeMap(ModifiedMRJob):
    DEFAULT_INPUT_PROTOCOL='raw_value'
    def mapper(self, key, line):
        data = parseData(line)
        d = data['t']
        if data: yield data['u'],'_'.join([getLidFromLocation(data['l']), str(d.weekday()), str(d.hour/4)])
    def reducer(self, user, occurences):
        locationMap = {}
        for s in occurences:
            location, day, dayBlock = s.split('_')
            if location not in locationMap: locationMap[location] = {}
            if day not in locationMap[location]: locationMap[location][day] = defaultdict(int)
            locationMap[location][day][dayBlock]+=1
        if len(locationMap)>10:  yield user, {'user': user, 'locations': locationMap}

if __name__ == '__main__':
    MRUserToLocationAndTimeMap.run()