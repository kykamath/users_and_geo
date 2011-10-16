'''
Created on Oct 12, 2011

@author: kykamath
'''
import sys
sys.path.append('../')
from mongo_settings import venuesCollection
from analysis.mr_location_user_day_mad import MRLocationUserDayMad
import random
from settings import validLocationAndUserHdfsPath, locationUserDayBlockMadFile,\
    spotsFIFolder, minLocationsTheUserHasCheckedin,\
    minUniqueUsersCheckedInTheLocation, locationUserDayMad
from library.file_io import FileIO
from library.plotting import getDataDistribution
from analysis.mr_location_user_dayblock_mad import MRLocationUserDayBlockMad
import matplotlib.pyplot as plt
from collections import defaultdict

spotsFile = '%s/%s_%s'%(spotsFIFolder, minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation)
spotsUserFile = spotsFile+'_users'

def runMRJob(mrJobClass, outputFileName, args='-r hadoop'.split(), **kwargs):
    mrJob = mrJobClass(args='-r hadoop'.split())
    for l in mrJob.runJob(inputFileList=[validLocationAndUserHdfsPath], **kwargs): FileIO.writeToFileAsJson(l[1], outputFileName)

def plotLocationDistribution(file, **conf):
    data = []
    scale = conf['scale']
    for d in FileIO.iterateJsonFromFile(file): data.append(d['location_db_mad'])
    plt.xlabel('locations mad'); plt.ylabel('# of locations')
    dataX, dataY = getDataDistribution(data)
    print dataX, dataY 
    plt.semilogy(dataX, dataY, marker='o', color='m')
#    plt.xlim(xmin=-0.1, xmax=2.6)
#    plt.ylim(ymax=10**6)
    locs, labels = plt.xticks()
    plt.xticks( locs, [x*2*scale+scale for x in locs] )
    plt.legend()
    plt.show()
    
def plotLocationToUserDistribution(file, **conf):
    data = defaultdict(list)
    scale = conf['scale']
    for d in FileIO.iterateJsonFromFile(file): data[d['location_db_mad']].append(d['users_db_mad'])
    plt.xlabel('users mad'); plt.ylabel('# of locations')
    j=1
    for i in sorted(data):
        plt.subplot(conf['row'], conf['column'],j)
        dataX, dataY = getDataDistribution(data[i])
        print i, dataX, dataY 
        plt.semilogy(dataX, dataY, marker='o', label='%s'%(i*2*scale+scale), color='m')
#        plt.xlim(xmin=-0.1, xmax=1.1)
#        plt.ylim(ymax=10**6)
#        locs, labels = plt.xticks()
#        plt.xticks( locs, [1]*len(locs) )
        locs, labels = plt.xticks()
        print locs
        plt.xticks( locs, [x*2*scale+scale for x in locs] )
        plt.legend()
        j+=1
    plt.show()

def getRandomLocationNames(file, **conf):
    data = defaultdict(list)
    for d in FileIO.iterateJsonFromFile(file): data[d['location_db_mad']].append(d['location'])
    for k in sorted(data):
        print k,
        for i in range(5):
            venue = venuesCollection.find_one({'lid':random.choice(data[k])})
            if venue: print unicode(venue['n']).encode("utf-8")+', ',
        print

if __name__ == '__main__':
    runMRJob(MRLocationUserDayBlockMad, locationUserDayBlockMadFile, jobconf={'mapred.reduce.tasks':5})
#    runMRJob(MRLocationUserDayMad, locationUserDayMad, jobconf={'mapred.reduce.tasks':5})

#    dayBlockConf = {'scale': 4, 'row': 2, 'column': 3}
#    dayConf = {'scale': 1, 'row': 3, 'column': 3}
#    plotLocationDistribution(locationUserDayBlockMadFile, **dayBlockConf)
#    plotLocationDistribution(locationUserDayMad, **dayConf)
#    plotLocationToUserDistribution(locationUserDayBlockMadFile, **dayBlockConf)
#    plotLocationToUserDistribution(locationUserDayMad, **dayConf)
#    getRandomLocationNames(locationUserDayBlockMadFile)
#    getRandomLocationNames(locationUserDayMad)
    