'''
Created on Oct 4, 2011

@author: kykamath
'''
checkinsFile = '/mnt/chevron/kykamath/data/geo/checkin_data.txt'
venuesFile = '/mnt/chevron/kykamath/data/geo/venues.txt'
validLocationAndUserFile = '/mnt/chevron/kykamath/data/geo/validLocationAndUserFile.txt'
checkinsHdfsPath = 'hdfs:///user/kykamath/geo/checkin_data.txt'
validLocationAndUserHdfsPath = 'hdfs:///user/kykamath/geo/validLocationAndUser.txt'
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
userToLocationAndTimeMapFile = analysisFolder+'userToLocationAndTimeMap'
locationToUserAndTimeMapFile = analysisFolder+'locationToUserAndTimeMap'
locationToUserAndExactTimeMapFile = analysisFolder+'locationToUserAndExactTimeMap'
filteredLocationToUserAndTimeMap_20_10 = analysisFolder+'filteredLocationToUserAndTimeMap_20_10'
locationGraph = '/mnt/chevron/kykamath/data/geo/analysis/locationGraph'
locationUserDayBlockMadFile = analysisFolder+'locationUserDayBlockMad'
locationUserDayMad = analysisFolder+'locationUserDayMad'
locationClustersFile = analysisFolder+'locationClusters'

#Analysis - Frequent location itemsets
locationsFIMahoutInputFile = '/mnt/chevron/kykamath/data/geo/analysis/fi/mh_input_%s_%s'
locationsFIMahoutOutputFile = '/mnt/chevron/kykamath/data/geo/analysis/fi/mh_output_min_user_locations_%s_%s_min_support_%s'

#Spots file
spotsFIFolder = '/mnt/chevron/kykamath/data/geo/analysis/spots/fi/'
spotsRadiusFolder = '/mnt/chevron/kykamath/data/geo/analysis/spots/radius/'
spotsUserGraphsFolder = '/mnt/chevron/kykamath/data/geo/analysis/spots/user_graphs/'
spotsFrequentItemsFolder = '/mnt/chevron/kykamath/data/geo/analysis/spots/frequent_items/'

#Brazos file
placesFolder = analysisFolder+'places/'
localPlacesFolder = '/data/geo/places/'
placesLocationToUserMapFile = localPlacesFolder+'%s/locationToUserMap'
placesARFFFile = localPlacesFolder+'%s/place.arff'
placesLocationWithClusterInfoFile = placesFolder+'%s/locationWithClusterInfo'
placesUserClustersFile = localPlacesFolder+'%s/userClusters'
placesUserClusterFeaturesFile = localPlacesFolder+'%s/userClusterFeatures'
placesLocationClustersFile = placesFolder+'%s/locationClusters'
placesImagesFolder = localPlacesFolder+'%s/images/'
placesGaussianImagesFolder = placesImagesFolder+'gaussian/'
placesTabsFolder = '/data/geo/places/tabs/'
placesKMLsFolder = '/data/geo/places/%s/kmls/'
placesAnalysisFolder = '/data/geo/places/%s/analysis/'

# Bondaries [[lower left][upper right]]
us_boundary = [[24.527135,-127.792969], [49.61071,-59.765625]]
brazos_valley_boundary = [[30.546887,-96.50322], [30.696973,-96.214828]]
austin_tx_boundary = [[30.097613,-97.971954], [30.486551,-97.535248]]
dallas_tx_boundary = [[32.735307,-96.862335], [32.886507,-96.723633]]
north_ca_boundary = [[37.068328,-122.640381], [37.924701,-122.178955]]
world_boundary = [[-90,-180], [90, 180]]

#Experiments
minLocationsTheUserHasCheckedin = 20
minUniqueUsersCheckedInTheLocation = 10
initialNumberofLocationsInSpot = 5 # The spot discovery algorithms use locations itemsets greater than this length to discover spots. 
minSupport = 3
minimumLocationsPerSpot = 4
locationRadiusInMiles = 10
radiusInMiles = 17
graphNodesMinEdgeWeight = 5 
graphNodesDistanceInMiles = 25
itemsetsMergeThreshold = 0.5

#Sequences
checkinSequenceGraphHdfsPath = 'hdfs:///user/kykamath/geo/checkinSequenceGraph'

checkinSequencesFolder = '/mnt/chevron/kykamath/data/geo/sequences/'
checkinSequenceGraphFile = checkinSequencesFolder+'checkinSequenceGraph'
checkinSequenceGraphLocationsFile = checkinSequencesFolder+'checkinSequenceGraphLocations'
