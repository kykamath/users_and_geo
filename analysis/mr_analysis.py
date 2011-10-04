'''
Created on Oct 4, 2011

@author: kykamath
'''
import sys
from library.file_io import FileIO
sys.path.append('../')
from settings import checkinsHdfsPath, analysisFolder, userDistributionFile,\
    locationDistributionFile
from analysis.mr_user_distribution import MRUserDistribution
from analysis.mr_user_to_location_map import MRUserToLocationMap
from analysis.mr_location_distribution import MRLocationDistribution
import matplotlib.pyplot as plt
from collections import defaultdict



def runMRJob(mrJobClass, outputFileName, args='-r hadoop'.split()):
    mrJob = mrJobClass(args='-r hadoop'.split())
    for l in mrJob.runJob(inputFileList=[checkinsHdfsPath]): FileIO.writeToFileAsJson(l[1], outputFileName)
    
def plotDistribution(inputFileName):
    dist = defaultdict(int)
    for data in FileIO.iterateJsonFromFile(inputFileName): dist[data['count']]+=1
    dataX, dataY = sorted(dist), [dist[x] for x in sorted(dist)]
    plt.loglog(dataX, dataY)
    plt.savefig('%s.pdf'%inputFileName.split('/')[-1]), plt.savefig('%s.eps'%inputFileName.split('/')[-1])

def locationIterator(minCheckins=10): return (data['location'] for data in FileIO.iterateJsonFromFile(locationDistributionFile) if data['count']>=minCheckins)
def userIterator(minCheckins=10): return (data['user'] for data in FileIO.iterateJsonFromFile(userDistributionFile) if data['count']>=minCheckins)

        

if __name__ == '__main__':
#    MR Jobs
#    runMRJob(MRUserDistribution, userDistributionFile)
#    runMRJob(MRLocationDistribution, locationDistributionFile)
    print len(list(locationIterator(minCheckins=0)))
    print len(list(userIterator(minCheckins=0)))
    print len(list(locationIterator()))
    print len(list(userIterator()))

#    Plots
#    plotDistribution(userDistributionFile)
#    plotDistribution(locationDistributionFile)