'''
Created on Nov 16, 2011

@author: kykamath
'''
from library.twitter import getDateTimeObjectFromTweetTimestamp
from library.mrjobwrapper import ModifiedMRJob
from library.geo import parseData, getLidFromLocation, isWithinBoundingBox, getLatticeLid
import cjson, time, datetime
from collections import defaultdict

#boundary = [[24.527135,-127.792969], [49.61071,-59.765625]] # US
#boundary = [[40.491, -74.356], [41.181, -72.612]] # NY

MINIMUM_NO_OF_CHECKINS_PER_LOCATION = 500


def getCheckinObject(line):
    data = cjson.decode(line)
    data['t'] = time.mktime(getDateTimeObjectFromTweetTimestamp(data['t']).timetuple())
    data['l'] = data['geo']; del data['geo']
    return data
def getDay(d): return datetime.date(d.year, d.month, d.day)
     
class MRHotSpots(ModifiedMRJob):
    DEFAULT_INPUT_PROTOCOL='raw_value'
    def map_rawData_to_latticeObjectUnits(self, key, line):
        data = getCheckinObject(line)
        yield getLatticeLid(data['l'], accuracy=0.001), data
        
    def map_rawData_to_reducedlatticeObjectUnits(self, key, line):
        data = getCheckinObject(line)
        data['u'] = data['user']['id']
        for k in ['tx', 'h', 'user']: del data[k] 
        yield getLatticeLid(data['l'], accuracy=0.001), data
    
    def reduce_latticeObjectUnits_to_latticeObjects(self, key, values):  
#        checkins = sorted([v for v in values], key=lambda c: c['t'])
        checkins = list(values)
        yield key, {'llid': key, 'c': checkins}
    
    def filter_latticeObjects(self, key, values):
        latticeObject = list(values)[0]
        total = len(latticeObject['c'])
        if total>=MINIMUM_NO_OF_CHECKINS_PER_LOCATION: yield key, latticeObject
    
    def split_checkins_in_latticeObject_by_day(self, key, latticeObject):
        checkins = latticeObject['c']
        checkinsDict = defaultdict(dict)
        for checkin in checkins: 
            checkinTime = datetime.datetime.fromtimestamp(checkin['t'])
            checkinHour, checkinDay = str(checkinTime.hour), str(time.mktime(getDay(checkinTime).timetuple()))
            if checkinHour not in checkinsDict[checkinDay]: checkinsDict[checkinDay][checkinHour] = 0
            checkinsDict[checkinDay][checkinHour]+=1   
        latticeObject['c'] = checkinsDict
        yield latticeObject['llid'], latticeObject
        
    def getJobsToGetFilteredLatticeObjects(self):
        return [
                self.mr(self.map_rawData_to_reducedlatticeObjectUnits, self.reduce_latticeObjectUnits_to_latticeObjects),
                self.mr(None, self.filter_latticeObjects),
                ]
        
    def getJobsToLatticeDailyCheckinDistribution(self): return self.getJobsToGetFilteredLatticeObjects() + [(self.split_checkins_in_latticeObject_by_day, None)]
    
    def steps(self):
        return self.getJobsToLatticeDailyCheckinDistribution()

if __name__ == '__main__':
    MRHotSpots.run()