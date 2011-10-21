'''
Created on Oct 21, 2011

@author: kykamath
'''
import sys
from library.file_io import FileIO
sys.path.append('../')
from mongo_settings import venuesCollection
from analysis.mr_analysis import locationIterator,\
    filteredLocationToUserAndTimeMapIterator
from library.geo import isWithinBoundingBox, getLocationFromLid
from settings import brazos_valley_boundary, minUniqueUsersCheckedInTheLocation,\
    minLocationsTheUserHasCheckedin, placesLocationToUserMapFile

place = ('brazos', brazos_valley_boundary)

def writeLocationToUserMap((name, boundary)):
    for location in filteredLocationToUserAndTimeMapIterator(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation):
        lid=getLocationFromLid(location['location'])
        if isWithinBoundingBox(lid, boundary): 
            title = venuesCollection.find_one({'lid':location['location']})
            if title: location['name'] = unicode(title['n']).encode("utf-8")
            else: location['name']=''
            for user in location['users'].keys()[:]: location['users'][str(user)]=location['users'][user]; del location['users'][user]
            FileIO.writeToFileAsJson(location, placesLocationToUserMapFile%name)

def locationToUserMapIterator((name, boundary)): return FileIO.iterateJsonFromFile(placesLocationToUserMapFile%name)
        
#writeLocationToUserMap(place)
for l in locationToUserMapIterator(place): print l
