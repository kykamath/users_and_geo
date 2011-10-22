'''
Created on Oct 3, 2011

@author: kykamath
'''
from library.mrjobwrapper import ModifiedMRJob
from library.geo import parseData, getLidFromLocation
from collections import defaultdict
import time

class MRLocationToUserAndExactTimeMap(ModifiedMRJob):
    DEFAULT_INPUT_PROTOCOL='raw_value'
    def mapper(self, key, line):
        data = parseData(line)
        if data: 
            d = data['t']
            yield getLidFromLocation(data['l']), '_'.join([str(data['u']), str(d.weekday()), str(d.hour/4), str(time.mktime(d.timetuple()))])
    def reducer(self, location, occurences):
        userMap = {}
        for s in occurences:
            user, day, dayBlock, epoch = s.split('_')
            if user not in userMap: userMap[user] = {}
            if day not in userMap[user]: userMap[user][day] = defaultdict(list)
            userMap[user][day][dayBlock].append(float(epoch))
        yield location, {'location': location, 'users': userMap}

if __name__ == '__main__':
    MRLocationToUserAndExactTimeMap.run()
