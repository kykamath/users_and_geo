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
userDistributionFile = analysisFolder+'userDistribution'
locationDistributionFile = analysisFolder+'locationDistribution'


# Bondaries [[lower left][upper right]]
us_boundary = [[24.527135,-127.792969], [49.61071,-59.765625]]
world_boundary = [[-90,-180], [90, 180]]
