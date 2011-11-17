'''
Created on Nov 16, 2011

@author: kykamath
'''
import sys, datetime
sys.path.append('../')
from collections import defaultdict
from hotspots.mr_checkinsByBoundary import MRCheckinsByBoundary
from hotspots.mr_buildLlidObjects import MRBuildLlidObjects
from settings import checkinsHdfsPath, regionsCheckinsFile, regionsCheckinsHdfsPath,\
    regionsLlidsFile
from library.file_io import FileIO
import matplotlib.pyplot as plt 

def day(d): return datetime.date(d.year, d.month, d.day)
def weekId(checkinTime): dateISO = datetime.datetime.fromtimestamp(checkinTime).isocalendar(); return '%s_%s'%(dateISO[0], dateISO[1])

def runMRJob(mrJobClass, outputFileName, inputFile=checkinsHdfsPath, args='-r hadoop'.split(), **kwargs):
    mrJob = mrJobClass(args='-r hadoop'.split())
    for l in mrJob.runJob(inputFileList=[inputFile], **kwargs): FileIO.writeToFileAsJson(l[1], outputFileName)
    
def analysis(region):
    total, i = 0, 1
#    dataDist = defaultdict(int)
    for location in FileIO.iterateJsonFromFile(regionsLlidsFile%region):
        if len(location['checkins'])>200:
            weekDist = defaultdict(int)
            weekIds = set()
            for checkin in location['checkins']:
                weekIds.add(weekId(checkin['t']))
                d = datetime.datetime.fromtimestamp(checkin['t'])
#                print d.hour
                weekDist[d.isoweekday()]+=1
#                dataDist[day(datetime.datetime.fromtimestamp(checkin['t']))]+=1
#            print i, location['llid'], len(location['checkins']); i+=1
#            print weekIds
#            print weekDist
            plt.plot(weekDist.keys(), [float(v)/len(weekIds) for v in weekDist.values()])
            plt.show()
#            exit()
#            total+=len(location['checkins'])
#            break
    print len(dataDist.keys()), len(dataDist.values())
    plt.plot_date(dataDist.keys(), dataDist.values())
    plt.savefig('dates.png')
    print total
    
if __name__ == '__main__':
    region='ny'
    runMRJob(MRCheckinsByBoundary, regionsCheckinsFile%region, jobconf={'mapred.reduce.tasks':50})
#    runMRJob(MRBuildLlidObjects, regionsLlidsFile%region, inputFile=regionsCheckinsHdfsPath%region, jobconf={'mapred.reduce.tasks':50})
#    analysis(region)