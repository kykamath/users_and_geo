'''
Created on Oct 4, 2011

@author: kykamath
'''
import sys
from library.file_io import FileIO
sys.path.append('../')
from settings import checkinsHdfsPath, analysisFolder
from analysis.mr_user_distribution import MRUserDistribution
from analysis.mr_user_to_location_map import MRUserToLocationMap

def runMRJob(mrJobClass, outputFileName, args='-r hadoop'.split()):
    mrJob = mrJobClass(args='-r hadoop'.split())
    for l in mrJob.runJob(inputFileList=[checkinsHdfsPath]): FileIO.writeToFileAsJson(l[1], outputFileName)

def userDistribution():
    mrUserDistribution = MRUserDistribution(args='-r hadoop'.split())
    for l in mrUserDistribution.runJob(inputFileList=['hdfs:///user/kykamath/geo/checkin_data.txt']):
        print l
        
#def userToLocationMap():
#    mrUserToLocationMap = MRUserToLocationMap(args='-r hadoop'.split())
#    for l in mrUserToLocationMap.runJob(inputFileList=['hdfs:///user/kykamath/geo/checkin_data.txt']):
#        print l

if __name__ == '__main__':
    runMRJob(MRUserDistribution, analysisFolder+'userDistribution')
#    userDistribution()
#    userToLocationMap()