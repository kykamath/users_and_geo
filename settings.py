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
minimumTransactionLength = 0
locationsFIMahoutInputFile = '/mnt/chevron/kykamath/data/geo/analysis/fi/mh_input_%s_%s'
locationsFIMahoutOutputFile = '/mnt/chevron/kykamath/data/geo/analysis/fi/mh_output_min_user_locations_%s_%s_min_support_%s'

# Bondaries [[lower left][upper right]]
us_boundary = [[24.527135,-127.792969], [49.61071,-59.765625]]
world_boundary = [[-90,-180], [90, 180]]

#Experiments
expMinimumCheckingsPerLocation = 10
expLocationRadiusInMiles = 10
expMinimumLocationsPerSpot = 3
expMinSharedUsersBetweenLocations = 5
