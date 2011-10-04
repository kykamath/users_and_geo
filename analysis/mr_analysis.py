'''
Created on Oct 4, 2011

@author: kykamath
'''
from analysis.mr_user_distribution import MRUserDistribution
def userDistribution():
    mrUserDistribution = MRUserDistribution(args='-r hadoop'.split())
    print mrUserDistribution.runJob(inputFileList=['hdfs:///user/kykamath/geo/checkin_data.txt'])
    
if __name__ == '__main__':
    userDistribution()