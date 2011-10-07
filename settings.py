'''
Created on Oct 4, 2011

@author: kykamath
'''
checkinsFile = '/mnt/chevron/kykamath/data/geo/checkin_data.txt'
venuesFile = '/mnt/chevron/kykamath/data/geo/venues.txt'
checkinsHdfsPath = 'hdfs:///user/kykamath/geo/checkin_data.txt'

#Analysis
analysisFolder = '/mnt/chevron/kykamath/data/geo/analysis/'
radiusSpotsFolder = '/mnt/chevron/kykamath/data/geo/analysis/radius_spots/'
userBasedSpotsFolder = '/mnt/chevron/kykamath/data/geo/analysis/user_based_spots/'
radiusSpotsKmlsFolder = '/Users/kykamath/Dropbox/data/geo/radius_spots/kmls/'
userBasedSpotsKmlsFolder = '/Users/kykamath/Dropbox/data/geo/user_based_spots/kmls/'
userDistributionFile = analysisFolder+'userDistribution'
locationDistributionFile = analysisFolder+'locationDistribution'
locationByUserDistributionFile = analysisFolder+'locationByUserDistribution'
userToLocationMapFile = analysisFolder+'userToLocationMapFile'
locationGraph = '/mnt/chevron/kykamath/data/geo/analysis/locationGraph'
#Analysis - Frequent location itemsets
minSupport = 3
minimumTransactionLength = 4
locationsFIMahoutInputFile = '/mnt/chevron/kykamath/data/geo/analysis/fi/mh_input'
locationsFIMahoutOutputFile = '/mnt/chevron/kykamath/data/geo/analysis/fi/mh_output'

# Bondaries [[lower left][upper right]]
us_boundary = [[24.527135,-127.792969], [49.61071,-59.765625]]
world_boundary = [[-90,-180], [90, 180]]
