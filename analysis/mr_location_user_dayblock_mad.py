'''
Created on Oct 12, 2011

@author: kykamath
'''
from library.mrjobwrapper import ModifiedMRJob
from library.math_modified import getMAD
import cjson

def getLocationUserSpecificMads(locationVector):
    completeDayBlockDistribution, madOfDayBlockDistributionForUsers = [], []
    for user in locationVector['users']:
        dayBlockDistributionForUser = []
        for day in locationVector['users'][user]:
            dayBlockDistributionForUser+=[int(dayBlock) for dayBlock in locationVector['users'][user][day] for i in range(locationVector['users'][user][day][dayBlock])]
        completeDayBlockDistribution+=dayBlockDistributionForUser
        madOfDayBlockDistributionForUsers.append(getMAD(dayBlockDistributionForUser) )
    return getMAD(madOfDayBlockDistributionForUsers), getMAD(completeDayBlockDistribution) 

class MRLocationUserDayBlockMad(ModifiedMRJob):
#    DEFAULT_INPUT_PROTOCOL='raw_value'
    def mapper(self, key, locationVector):
#        locationVector = cjson.decode(locationVector)
        usersMad, locationMad = getLocationUserSpecificMads(locationVector)
        yield locationVector['location'],  {'location': locationVector['location'], 'no_users': len(locationVector['users']),'users_db_mad': usersMad, 'location_db_mad': locationMad}
#    def reducer(self, location, occurrences): yield location, {'location': location, 'count':list(occurrences)[0]}

if __name__ == '__main__':
    MRLocationUserDayBlockMad.run()