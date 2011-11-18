'''
Created on Nov 16, 2011

@author: kykamath
'''
from library.twitter import getDateTimeObjectFromTweetTimestamp
from library.mrjobwrapper import ModifiedMRJob
from library.geo import parseData, getLidFromLocation, isWithinBoundingBox, getLatticeLid,\
    getLocationFromLid
import cjson, time, datetime
from collections import defaultdict

ACCURACY = 0.001
NO_OF_HASHTAGS_PER_LATTICE = 10

#BOUNDARY = [[24.527135,-127.792969], [49.61071,-59.765625]] # US
#MINIMUM_NO_OF_CHECKINS_PER_LOCATION = 1

BOUNDARY = [[40.491, -74.356], [41.181, -72.612]] # NY
MINIMUM_NO_OF_CHECKINS_PER_LOCATION = 500

def getCheckinObject(line):
    data = cjson.decode(line)
    data['t'] = time.mktime((getDateTimeObjectFromTweetTimestamp(data['t'])-datetime.timedelta(hours=5)).timetuple())
    data['l'] = data['geo']; del data['geo']
    return data
def getDay(d): return datetime.date(d.year, d.month, d.day)
def getMergeDay(d):
    weekDay = d.weekday()
    if weekDay in [0,1,2]: return getDay(d-datetime.timedelta(days=weekDay))
    elif weekDay in [3,4]: return getDay(d-datetime.timedelta(days=weekDay-3))
    else: return getDay(d-datetime.timedelta(days=weekDay-5))
def getMergedCheckinHour(h): return h/4

class MRHotSpots(ModifiedMRJob):
    DEFAULT_INPUT_PROTOCOL='raw_value'
    def map_rawData_to_latticeObjectUnits(self, key, line):
        data = getCheckinObject(line)
        yield getLatticeLid(data['l'], accuracy=ACCURACY), data
        
    def map_rawData_to_reducedlatticeObjectUnits(self, key, line):
        data = getCheckinObject(line)
        data['u'] = data['user']['id']
        for k in ['tx', 'user']: del data[k] 
        yield getLatticeLid(data['l'], accuracy=0.001), data
    
    def reduce_latticeObjectUnits_to_latticeObjects(self, key, values):  
#        checkins = sorted([v for v in values], key=lambda c: c['t'])
        checkins, hashtags = list(values), defaultdict(int)
        for c in checkins[:]:
            for h in c['h']: hashtags[h]+=1
        yield key, {'llid': key, 'c': checkins, 'tc': len(checkins), 'h': sorted(hashtags.iteritems(), key=lambda i: i[1], reverse=True)[:NO_OF_HASHTAGS_PER_LATTICE]}
    
    def filter_latticeObjects(self, key, values):
        latticeObject = list(values)[0]
        total = len(latticeObject['c'])
        if total>=MINIMUM_NO_OF_CHECKINS_PER_LOCATION and \
            isWithinBoundingBox(getLocationFromLid(latticeObject['llid'].replace('_', ' ')), BOUNDARY): 
            yield key, latticeObject
    
    def get_lattice_descriptions(self, key, latticeObject):
        checkinsDist = dict((str(i),0.) for i in range(24))
        for checkin in latticeObject['c']: checkinsDist[str(datetime.datetime.fromtimestamp(checkin['t']).hour)]+=1
        latticeObject['dayDist'] = checkinsDist; del latticeObject['c']
        yield latticeObject['llid'], latticeObject
    
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
        
    def split_checkins_in_latticeObject_by_smoothedDay(self, key, latticeObject):
        checkins = latticeObject['c']
        checkinsDict = defaultdict(dict)
        for checkin in checkins: 
            checkinTime = datetime.datetime.fromtimestamp(checkin['t'])
#            checkinHour, checkinDay = str(getMergedCheckinHour(checkinTime.hour)), str(time.mktime(getMergeDay(checkinTime).timetuple()))
            checkinHour, checkinDay = str(checkinTime.hour), str(time.mktime(getMergeDay(checkinTime).timetuple()))
            if checkinHour not in checkinsDict[checkinDay]: checkinsDict[checkinDay][checkinHour] = 0
            checkinsDict[checkinDay][checkinHour]+=1   
        latticeObject['c'] = checkinsDict
        yield latticeObject['llid'], latticeObject
    
    def get_checkinDistribution(self, key, latticeObject): yield key, {'llid': latticeObject['llid'], 'count': len(latticeObject['c'])}
        
    def getJobsToGetFilteredLatticeObjects(self):
        return [
                self.mr(self.map_rawData_to_reducedlatticeObjectUnits, self.reduce_latticeObjectUnits_to_latticeObjects),
                self.mr(None, self.filter_latticeObjects),
                ]
    
    def getJobsToGetCheckinDistribution(self): return self.getJobsToGetFilteredLatticeObjects() + [(self.get_checkinDistribution, None)]    
    def getJobsToGetLatticeDescription(self): return self.getJobsToGetFilteredLatticeObjects() + [(self.get_lattice_descriptions, None)]
    def getJobsToLatticeDailyCheckinDistribution(self): return self.getJobsToGetFilteredLatticeObjects() + [(self.split_checkins_in_latticeObject_by_day, None)]
    def getJobsToLatticeSmoothedDailyCheckinDistribution(self): return self.getJobsToGetFilteredLatticeObjects() + [(self.split_checkins_in_latticeObject_by_smoothedDay, None)]
    
    def steps(self):
#        return self.getJobsToGetCheckinDistribution()
        return self.getJobsToGetLatticeDescription() 
#        return self.getJobsToLatticeDailyCheckinDistribution()
#        return self.getJobsToLatticeSmoothedDailyCheckinDistribution()

if __name__ == '__main__':
    MRHotSpots.run()