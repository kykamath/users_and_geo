'''
Created on Oct 4, 2011

@author: kykamath
'''
import sys
sys.path.append('../')
from settings import checkinsHdfsPath, analysisFolder, userDistributionFile,\
    locationDistributionFile, locationGraph, locationByUserDistributionFile,\
    userToLocationMapFile, userToLocationAndTimeMapFile
from library.file_io import FileIO
from analysis.mr_location_by_user_distribution import MRLocationByUserDistribution
from analysis.mr_user_to_location_and_time_map import MRUserToLocationAndTimeMap
from analysis.mr_user_distribution import MRUserDistribution
from analysis.mr_user_to_location_map import MRUserToLocationMap
from analysis.mr_location_distribution import MRLocationDistribution
from analysis.mr_location_graph_by_users import MRLocationGraphByUsers
import matplotlib.pyplot as plt
from collections import defaultdict
from library.plotting import Map, getDataDistribution



def runMRJob(mrJobClass, outputFileName, args='-r hadoop'.split(), **kwargs):
    mrJob = mrJobClass(args='-r hadoop'.split())
    for l in mrJob.runJob(inputFileList=[checkinsHdfsPath], **kwargs): FileIO.writeToFileAsJson(l[1], outputFileName)
    
def plotDistribution(inputFileName):
    dist = defaultdict(int)
    for data in FileIO.iterateJsonFromFile(inputFileName): dist[data['count']]+=1
    dataX, dataY = sorted(dist), [dist[x] for x in sorted(dist)]
    plt.loglog(dataX, dataY)
    plt.savefig('%s.pdf'%inputFileName.split('/')[-1]), plt.savefig('%s.eps'%inputFileName.split('/')[-1])
def plotLocationGraphEdgeDistribution():
    dataX, dataY = getDataDistribution(edge['w'] for edge in FileIO.iterateJsonFromFile(locationGraph))
    plt.loglog(dataX, dataY)
    plt.savefig('%s.pdf'%'locationGraph')

def locationIterator(minCheckins=10, fullRecord = False): 
    if fullRecord: return (data for data in FileIO.iterateJsonFromFile(locationDistributionFile) if data['count']>=minCheckins)
    return (data['location'] for data in FileIO.iterateJsonFromFile(locationDistributionFile) if data['count']>=minCheckins)
def userIterator(minCheckins=10): return (data['user'] for data in FileIO.iterateJsonFromFile(userDistributionFile) if data['count']>=minCheckins)
def userToLocationMapIterator(minLocations, fullRecord): 
    if fullRecord: return (data for data in FileIO.iterateJsonFromFile(userToLocationMapFile) if len(data['locations'])>minLocations)
    return (data['locations'] for data in FileIO.iterateJsonFromFile(userToLocationMapFile) if len(data['locations'])>minLocations)
def locationGraphIterator(minimumWeight=0): return (d for d in FileIO.iterateJsonFromFile(locationGraph) if d['w']>=minimumWeight)
def locationByUserDistributionIterator(minTimesUserCheckedIn, fullRecord=False): 
    if fullRecord: return (data for data in FileIO.iterateJsonFromFile(locationByUserDistributionFile) if data['count']>=minTimesUserCheckedIn)
    return (data['location'] for data in FileIO.iterateJsonFromFile(locationByUserDistributionFile) if data['count']>=minTimesUserCheckedIn)
def filteredUserIterator(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation, fullRecord = False):
    ''' Iterates user vectors who have checked in into at-least minUniqueUsersCheckedIn places.
    Dimensions are places that have been checked in by atleast minUniqueUsersCheckedIn users. 
    '''
    validLocationsSet = set(locationByUserDistributionIterator(minUniqueUsersCheckedInTheLocation))
    for userVector in userToLocationMapIterator(minLocationsTheUserHasCheckedin, fullRecord):
        for k in userVector.keys()[:]:
            if k not in validLocationsSet: del userVector[k]
        if userVector: yield userVector

if __name__ == '__main__':
#    MR Jobs
#    runMRJob(MRUserDistribution, userDistributionFile)
#    runMRJob(MRLocationDistribution, locationDistributionFile)
#    runMRJob(MRLocationByUserDistribution, locationByUserDistributionFile)
#    runMRJob(MRLocationGraphByUsers, locationGraph)
#    runMRJob(MRUserToLocationMap, userToLocationMapFile, jobconf={'mapred.reduce.tasks':5})
    runMRJob(MRUserToLocationAndTimeMap, userToLocationAndTimeMapFile, jobconf={'mapred.reduce.tasks':5})

#    Plots
#    plotDistribution(userDistributionFile)
#    plotDistribution(locationDistributionFile)
#    plotDistribution(locationByUserDistributionFile)
#    plotLocationGraphEdgeDistribution()
    
#    print len(list(locationByUserDistributionIterator(minTimesUserCheckedIn=10)))
#    i = 0
    for i in filteredUserIterator(10, 10, True):
        print i
