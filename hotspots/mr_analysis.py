'''
Created on Nov 16, 2011

@author: kykamath
'''
import sys
sys.path.append('../')
from hotspots.mr_checkinsByBoundary import MRCheckinsByBoundary
from settings import checkinsHdfsPath, nyCheckinsFile
from library.file_io import FileIO

def runMRJob(mrJobClass, outputFileName, args='-r hadoop'.split(), **kwargs):
    mrJob = mrJobClass(args='-r hadoop'.split())
    for l in mrJob.runJob(inputFileList=[checkinsHdfsPath], **kwargs): FileIO.writeToFileAsJson(l[1], outputFileName)
    
if __name__ == '__main__':
    runMRJob(MRCheckinsByBoundary, nyCheckinsFile, jobconf={'mapred.reduce.tasks':50})