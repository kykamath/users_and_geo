'''
Created on Nov 16, 2011

@author: kykamath
'''
import sys
sys.path.append('../')
from hotspots.mr_checkinsByBoundary import MRCheckinsByBoundary
from hotspots.mr_buildLlidObjects import MRBuildLlidObjects
from settings import checkinsHdfsPath, regionsCheckinsFile, regionsCheckinsHdfsPath,\
    regionsLlidsFile
from library.file_io import FileIO

def runMRJob(mrJobClass, outputFileName, inputFile=checkinsHdfsPath, args='-r hadoop'.split(), **kwargs):
    mrJob = mrJobClass(args='-r hadoop'.split())
    for l in mrJob.runJob(inputFileList=[inputFile], **kwargs): FileIO.writeToFileAsJson(l[1], outputFileName)
    
def analysis(region):
    for location in FileIO.iterateJsonFromFile(regionsLlidsFile%region):
        print location['llid'], len(location['checkins'])
    
if __name__ == '__main__':
    region='ny'
#    runMRJob(MRCheckinsByBoundary, regionsCheckinsFile%region, jobconf={'mapred.reduce.tasks':50})
#    runMRJob(MRBuildLlidObjects, regionsLlidsFile%region, inputFile=regionsCheckinsHdfsPath%region, jobconf={'mapred.reduce.tasks':50})
    analysis(region)