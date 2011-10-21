'''
Created on Oct 21, 2011

@author: kykamath
'''
import sys
sys.path.append('../')
from mongo_settings import venuesCollection
from analysis.mr_analysis import locationIterator,\
    filteredLocationToUserAndTimeMapIterator
from library.geo import isWithinBoundingBox, getLocationFromLid
from settings import brazos_valley_boundary, minUniqueUsersCheckedInTheLocation,\
    minLocationsTheUserHasCheckedin


place = ('brazos', brazos_valley_boundary)
i=0
for location in filteredLocationToUserAndTimeMapIterator(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation):
    lid=getLocationFromLid(location['location'])
    if isWithinBoundingBox(lid, place[1]): 
        title = venuesCollection.find_one({'lid':lid})
        print title
#        if title: location['name'] = unicode(title['n']).encode("utf-8")
#        else: location['name']=''
#        print i, location; i+=1
#    print location
#    exit()