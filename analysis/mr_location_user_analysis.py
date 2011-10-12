'''
Created on Oct 12, 2011

@author: kykamath
'''
from settings import validLocationAndUserHdfsPath, locationUserDayBlockMadFile
from library.file_io import FileIO
from analysis.mr_location_user_dayblock_mad import MRLocationUserDayBlockMad

def runMRJob(mrJobClass, outputFileName, args='-r hadoop'.split(), **kwargs):
    mrJob = mrJobClass(args='-r hadoop'.split())
    for l in mrJob.runJob(inputFileList=[validLocationAndUserHdfsPath], **kwargs): FileIO.writeToFileAsJson(l[1], outputFileName)

if __name__ == '__main__':
    runMRJob(MRLocationUserDayBlockMad, locationUserDayBlockMadFile, jobconf={'mapred.reduce.tasks':5})