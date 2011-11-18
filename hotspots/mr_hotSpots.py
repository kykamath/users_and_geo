'''
Created on Nov 16, 2011

@author: kykamath
'''
from library.twitter import getDateTimeObjectFromTweetTimestamp
from library.mrjobwrapper import ModifiedMRJob
from library.geo import parseData, getLidFromLocation, isWithinBoundingBox, getLatticeLid,\
    getLocationFromLid
import cjson, time, datetime, math
from collections import defaultdict

ACCURACY = 0.001
NO_OF_HASHTAGS_PER_LATTICE = 10
MIN_HASHTAG_OCCURANCES_PER_LATTICE = 10

#BOUNDARY = [[24.527135,-127.792969], [49.61071,-59.765625]] # US
#MINIMUM_NO_OF_CHECKINS_PER_LOCATION = 2

#BOUNDARY = [[40.491, -74.356], [41.181, -72.612]] # NY
#MINIMUM_NO_OF_CHECKINS_PER_LOCATION = 500

BOUNDARY = [[30.546887,-96.50322], [30.696973,-96.214828]] #Brazos, TX
MINIMUM_NO_OF_CHECKINS_PER_LOCATION = 25

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
    def emptyMapper(self, key, line): yield key, line
    
    def map_to_count_number_of_lattices(self, key, latticeObject): yield 1, latticeObject
    
    def reduce_to_count_number_of_lattices(self, key, values):
        lattices = list(values)
        numberOfLattices = len(lattices)
        for lattice in lattices:
            lattice['noOfLattices'] = numberOfLattices
            yield lattice['llid'], lattice
    
    def map_rawData_to_latticeObjectUnits(self, key, line):
        data = getCheckinObject(line)
        yield getLatticeLid(data['l'], accuracy=ACCURACY), data
        
    def map_rawData_to_reducedlatticeObjectUnits(self, key, line):
        data = getCheckinObject(line)
        data['u'] = data['user']['id']
        for k in ['tx', 'user']: del data[k] 
        yield getLatticeLid(data['l'], accuracy=ACCURACY), data
    
    def reduce_latticeObjectUnits_to_latticeObjects(self, key, values):  
#        checkins = sorted([v for v in values], key=lambda c: c['t'])
        checkins, hashtags = list(values), defaultdict(int)
        for c in checkins[:]:
            for h in c['h']: hashtags[h.lower()]+=1
        for k in hashtags.keys()[:]:
            if hashtags[k]<MIN_HASHTAG_OCCURANCES_PER_LATTICE: del hashtags[k] 
        yield key, {'llid': key, 'c': checkins, 'tc': len(checkins), 'h': sorted(hashtags.iteritems(), key=lambda i: i[1], reverse=True)}
    
    def filter_latticeObjects(self, key, values):
        latticeObject = list(values)[0]
        total = len(latticeObject['c'])
        if total>=MINIMUM_NO_OF_CHECKINS_PER_LOCATION and \
            isWithinBoundingBox(getLocationFromLid(latticeObject['llid'].replace('_', ' ')), BOUNDARY): 
            yield key, latticeObject
    
    def filter_latticeObjectsByBoundaryOny(self, key, values):
        latticeObject = list(values)[0]
        if isWithinBoundingBox(getLocationFromLid(latticeObject['llid'].replace('_', ' ')), BOUNDARY): 
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
    
    ''' Start: Methods to determine hastag scores per lattice using Okapi BM25 (http://en.wikipedia.org/wiki/Okapi_BM25)
    '''
    def map_hashtags_to_lids(self, key, latticeObject):
        for hashtag, occurences in latticeObject['h']: yield hashtag, {'llid': latticeObject['llid'], 'totChk': latticeObject['tc'], 'hc': occurences, 'noOfLattices': latticeObject['noOfLattices']}
    
    def get_hashtags_idf_and_pass_it_to_llids(self, key, values):
#        def calculateIdf(df, lattice): return math.log((lattice['noOfLattices']-df+0.5)/(df+0.5))
        def calculateIdf(df, lattice): return math.log(lattice['noOfLattices']/float(df))
        lattices = list(values)
        idf = None
        df = len(lattices)
        for lattice in lattices: 
            if idf==None: idf = calculateIdf(df, lattice)
            lattice['h'] = {key: [lattice['hc'], idf]}; del lattice['hc']
            yield lattice['llid'], lattice
    
    def combine_all_hastags_idf_for_lattice_and_calculate_bm25_scores(self, key, values):
#        def calculateBM25(f_h_l, idf, k1=1.5, b=0.75): 
#            tf = (f_h_l*(k1+1))/(f_h_l+k1*(1-2*b))
#            return idf*math.log(tf)
        def tf_idf(f_h_l, idf): return idf*math.log(f_h_l)
        hashtags = {}
        currentLatticeInstance = None
        for latticeInstance in values:
#            for k, v in latticeInstance['h'].iteritems(): hashtags[k] = tf_idf(v[0], v[1]) 
            for k, v in latticeInstance['h'].iteritems(): hashtags[k] = v[1] 
            currentLatticeInstance = latticeInstance
        currentLatticeInstance['h'] = sorted(hashtags.iteritems(), key=lambda i: i[1], reverse=True)[:NO_OF_HASHTAGS_PER_LATTICE]
        if currentLatticeInstance['totChk'] >= MINIMUM_NO_OF_CHECKINS_PER_LOCATION: yield currentLatticeInstance['llid'], currentLatticeInstance
    ''' End: Methods to determine hastag scores per lattice using Okapi BM25 (http://en.wikipedia.org/wiki/Okapi_BM25)
    '''
            
    def jobsToGetFilteredLatticeObjects(self):
        return [
                self.mr(self.map_rawData_to_reducedlatticeObjectUnits, self.reduce_latticeObjectUnits_to_latticeObjects),
                self.mr(None, self.filter_latticeObjects),
                ]
        
    def jobsToGetLatticesFilteredObjectsByBoundaryOnly(self):
        return [
                self.mr(self.map_rawData_to_reducedlatticeObjectUnits, self.reduce_latticeObjectUnits_to_latticeObjects),
                self.mr(None, self.filter_latticeObjectsByBoundaryOny),
                ]
    
    def jobsToGetCheckinDistribution(self): return self.jobsToGetFilteredLatticeObjects() + [(self.get_checkinDistribution, None)]
    def jobsToCountNumberOfLattices(self): return [(self.map_to_count_number_of_lattices, self.reduce_to_count_number_of_lattices)]
    def jobsToGetLatticeDescription(self): return self.jobsToGetFilteredLatticeObjects() + [(self.get_lattice_descriptions, None)]
    def jobsToLatticeDailyCheckinDistribution(self): return self.jobsToGetFilteredLatticeObjects() + [(self.split_checkins_in_latticeObject_by_day, None)]
    def jobsToLatticeSmoothedDailyCheckinDistribution(self): return self.jobsToGetFilteredLatticeObjects() + [(self.split_checkins_in_latticeObject_by_smoothedDay, None)]
    def jobsToGetLatticeSpecificHashtags(self): return self.jobsToGetLatticesFilteredObjectsByBoundaryOnly() + \
                                                        self.jobsToCountNumberOfLattices() + \
                                                        [
                                                         (self.map_hashtags_to_lids, self.get_hashtags_idf_and_pass_it_to_llids),
                                                         (self.emptyMapper, self.combine_all_hastags_idf_for_lattice_and_calculate_bm25_scores)
                                                         ]
    
    def steps(self):
#        return self.jobsToGetCheckinDistribution()
        return self.jobsToGetLatticeSpecificHashtags()
#        return self.jobsToGetLatticeDescription() 
#        return self.jobsToLatticeDailyCheckinDistribution()
#        return self.jobsToLatticeSmoothedDailyCheckinDistribution()

if __name__ == '__main__':
    MRHotSpots.run()