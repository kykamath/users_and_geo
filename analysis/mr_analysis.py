'''
Created on Oct 4, 2011

@author: kykamath
'''
import sys
sys.path.append('../')
from analysis.mr_user_distribution import MRUserDistribution
from analysis.mr_user_to_location_map import MRUserToLocationMap

def userDistribution():
    mrUserDistribution = MRUserDistribution(args='-r hadoop'.split())
    for l in mrUserDistribution.runJob(inputFileList=['hdfs:///user/kykamath/geo/checkin_data.txt']):
        print l
def userToLocationMap():
    mrUserToLocationMap = MRUserToLocationMap(args='-r hadoop'.split())
    for l in mrUserToLocationMap.runJob(inputFileList=['hdfs:///user/kykamath/geo/checkin_data.txt']):
        print l

if __name__ == '__main__':
#    userDistribution()
    userToLocationMap()