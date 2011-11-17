'''
Created on Nov 16, 2011

@author: kykamath
'''
import sys
sys.path.append('../')
from hotspots.mr_checkinsByBoundary import MRCheckinsByBoundary
from hotspots.mr_buildLlidObjects import MRBuildLlidObjects
from settings import checkinsHdfsPath, regionsCheckinsFile, regionsCheckinsHdfsPath
from library.file_io import FileIO

def runMRJob(mrJobClass, outputFileName, inputFile=checkinsHdfsPath, args='-r hadoop'.split(), **kwargs):
    mrJob = mrJobClass(args='-r hadoop'.split())
    for l in mrJob.runJob(inputFileList=[inputFile], **kwargs): FileIO.writeToFileAsJson(l[1], outputFileName)
    
if __name__ == '__main__':
    region='ny'
#    runMRJob(MRCheckinsByBoundary, regionsCheckinsFile%region, jobconf={'mapred.reduce.tasks':50})
    runMRJob(MRBuildLlidObjects, regionsCheckinsFile%region, inputFile=regionsCheckinsHdfsPath%region, jobconf={'mapred.reduce.tasks':50})