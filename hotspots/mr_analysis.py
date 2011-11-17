'''
Created on Nov 16, 2011

@author: kykamath
'''
import sys, datetime
sys.path.append('../')
from collections import defaultdict
from hotspots.mr_hotSpots import MRHotSpots
from hotspots.mr_checkinsByBoundary import MRCheckinsByBoundary
from hotspots.mr_buildLlidObjects import MRBuildLlidObjects
from settings import checkinsHdfsPath, regionsCheckinsFile, regionsCheckinsHdfsPath,\
    regionsLlidsFile, dailyDistribution, twitterCheckinsFileInHDFS,\
    checkinsDistribution
from library.file_io import FileIO
import matplotlib.pyplot as plt 

def getDay(d): return datetime.date(d.year, d.month, d.day)
def getClosestMonday(d): return getDay(d-datetime.timedelta(days=d.weekday()))
#    try:
#        return datetime.date(d.year, d.month, d.day-d.weekday())
#    except ValueError: 
#        print 'x'
#        return datetime.date(d.year, d.month-1, d.day-d.weekday()) 
#    return '%s_%s'%(dateISO[0], dateISO[1])

def runMRJob(mrJobClass, outputFileName, inputFile=checkinsHdfsPath, args='-r hadoop'.split(), **kwargs):
    mrJob = mrJobClass(args='-r hadoop'.split())
    for l in mrJob.runJob(inputFileList=[inputFile], **kwargs): FileIO.writeToFileAsJson(l[1], outputFileName)
    
#def locationIterator(region, minCheckins=1000):
#    for location in FileIO.iterateJsonFromFile(regionsLlidsFile%region):
#        if len(location['checkins'])>minCheckins: yield location
#    
#def basicStats(region):
#    i, total = 0, 0
#    for location in locationIterator(region):
#        print i, len(location['checkins']); i+=1
#        total+=len(location['checkins'])
#    print total
#
#def locationAnalysis(location):
#    dayDist = defaultdict(int)
#    weekDist = defaultdict(int)
#    for checkin in location['checkins']:
#        d = datetime.datetime.fromtimestamp(checkin['t'])
#        dayDist[getDay(d)]+=1
#        weekDist[getClosestMonday(d)]+=1
#    dataX = sorted(weekDist.keys())
#    print len(location['checkins'])
#    plt.plot_date(dataX, [weekDist[d] for d in dataX])
#    plt.show()
##    print dayDist
##    exit()    
#    
#def analysis(region):
#    total, i = 0, 1
##    dataDist = defaultdict(int)
#    for location in FileIO.iterateJsonFromFile(regionsLlidsFile%region):
#        if len(location['checkins'])>200:
#            weekDist = defaultdict(int)
#            weekIds = set()
#            for checkin in location['checkins']:
#                weekIds.add(weekId(checkin['t']))
#                d = datetime.datetime.fromtimestamp(checkin['t'])
##                print d.hour
#                weekDist[d.isoweekday()]+=1
##                dataDist[day(datetime.datetime.fromtimestamp(checkin['t']))]+=1
##            print i, location['llid'], len(location['checkins']); i+=1
##            print weekIds
##            print weekDist
#            plt.plot(weekDist.keys(), [float(v)/len(weekIds) for v in weekDist.values()])
#            plt.show()
##            exit()
##            total+=len(location['checkins'])
##            break
#    print len(dataDist.keys()), len(dataDist.values())
#    plt.plot_date(dataDist.keys(), dataDist.values())
#    plt.savefig('dates.png')
#    print total
    
def plotDailyDistributionForLattices():
    for l in FileIO.iterateJsonFromFile(dailyDistribution):
        distForLattice = dict([(str(i), 0.) for i in range(24)])
        checkinsByDay = l['c']
        days = sorted([datetime.datetime.fromtimestamp(float(d)) for d in checkinsByDay])
        noOfDays = (days[-1]-days[0]).days
        for day, dist in checkinsByDay.iteritems():
            for h, v in dist.iteritems(): distForLattice[h]+=v
        dataX = sorted([int(i) for i in distForLattice])
        plt.plot(dataX, [distForLattice[str(k)]/noOfDays for k in dataX])
        plt.show()
#        exit()
    
if __name__ == '__main__':
    region='ny'
    month = 2
    month = '2_5'
#    runMRJob(MRCheckinsByBoundary, regionsCheckinsFile%region, jobconf={'mapred.reduce.tasks':50})
#    runMRJob(MRBuildLlidObjects, regionsLlidsFile%region, inputFile=regionsCheckinsHdfsPath%region, jobconf={'mapred.reduce.tasks':50})

    runMRJob(MRHotSpots, checkinsDistribution, inputFile=twitterCheckinsFileInHDFS%month, jobconf={'mapred.reduce.tasks':50})


#    plotDailyDistributionForLattices()
