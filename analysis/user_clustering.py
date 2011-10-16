'''
Created on Oct 15, 2011

@author: kykamath
'''
import sys
sys.path.append('../')
from analysis.mr_analysis import filteredUserIterator,\
    filteredLocationToUserAndTimeMapIterator
from settings import minLocationsTheUserHasCheckedin,\
    minUniqueUsersCheckedInTheLocation
from operator import itemgetter
def getUserVectors():
    ''' Returns a dict for user vectors across top 100 location dimensions.
    '''
    return dict((u['user'], dict(sorted(u['locations'].iteritems(), key=itemgetter(1), reverse=True)[:100])) for u in filteredUserIterator(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation,  fullRecord = True))
#userVectors = getUserVectors()

for l in filteredLocationToUserAndTimeMapIterator(minLocationsTheUserHasCheckedin, minUniqueUsersCheckedInTheLocation):
    print l