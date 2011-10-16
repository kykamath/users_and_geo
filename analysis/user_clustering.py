'''
Created on Oct 15, 2011

@author: kykamath
'''
import sys
sys.path.append('../')
from analysis.mr_analysis import filteredUserIterator,\
    filteredLocationToUserAndTimeMapIterator, locationsForUsIterator
from settings import minLocationsTheUserHasCheckedin,\
    minUniqueUsersCheckedInTheLocation
from operator import itemgetter
from library.plotting import getDataDistribution

def getUserVectors():
    ''' Returns a dict for user vectors across top 100 location dimensions.
    '''
    return dict((u['user'], dict(sorted(u['locations'].iteritems(), key=itemgetter(1), reverse=True)[:100])) for u in filteredUserIterator(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation,  fullRecord = True))
#userVectors = getUserVectors()

def getDayBlockDistributionForUsers(locationVector):
    completeDayBlockDistribution = []
    for user in locationVector['users']:
        dayBlockDistributionForUser = []
        for day in locationVector['users'][user]:
            dayBlockDistributionForUser+=[int(dayBlock) for dayBlock in locationVector['users'][user][day] for i in range(locationVector['users'][user][day][dayBlock])]
        completeDayBlockDistribution+=dayBlockDistributionForUser
    dataX, dataY = getDataDistribution(completeDayBlockDistribution)
    print dict((str(x),y) for x,y in zip(dataX, dataY))

locationsInUS = set(list(locationsForUsIterator(minUniqueUsersCheckedInTheLocation)))
for location in filter(lambda l: l['location'] in locationsInUS, filteredLocationToUserAndTimeMapIterator(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation)): 
    getDayBlockDistributionForUsers(location['users'])
    
