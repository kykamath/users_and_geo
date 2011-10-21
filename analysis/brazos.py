'''
Created on Oct 21, 2011

@author: kykamath
'''
import sys
sys.path.append('../')
from analysis.mr_analysis import locationIterator,\
    filteredLocationToUserAndTimeMapIterator
from library.geo import isWithinBoundingBox, getLocationFromLid
from settings import brazos_valley_boundary, minUniqueUsersCheckedInTheLocation,\
    minLocationsTheUserHasCheckedin

i=0
for location in filteredLocationToUserAndTimeMapIterator(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation):
    if isWithinBoundingBox(getLocationFromLid(location['location']), brazos_valley_boundary): print i, location; i+=1
#    print location
#    exit()