'''
Created on Nov 12, 2011

@author: kykamath
'''
import sys
sys.path.append('../')
from checkin_sequence_analysis.mr_buildSequenceAdjacencyList import MRBuildCheckinSequenceAdjacencyList
from settings import checkinSequenceGraphHdfsPath,\
    checkinSequenceGraphLocationsFile
from library.file_io import FileIO

def runMRJob(mrJobClass, outputFileName, args='-r hadoop'.split(), **kwargs):
    mrJob = mrJobClass(args='-r hadoop'.split())
    for l in mrJob.runJob(inputFileList=[checkinSequenceGraphHdfsPath], **kwargs): FileIO.writeToFileAsJson(l[1], outputFileName)
    
if __name__ == '__main__':
    runMRJob(MRBuildCheckinSequenceAdjacencyList, checkinSequenceGraphLocationsFile, jobconf={'mapred.reduce.tasks':50})