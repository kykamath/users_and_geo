'''
Created on Oct 12, 2011

@author: kykamath
'''
from library.mrjobwrapper import ModifiedMRJob
from library.math_modified import getMAD
import cjson
from library.file_io import FileIO

def getLocationUserSpecificMads(locationVector):
    completeDayDistribution, madOfDayDistributionForUsers = [], []
    for user in locationVector['users']:
        dayDistributionForUser = []
        for day in locationVector['users'][user]:
            dayDistributionForUser+=[int(day) for day in locationVector['users'][user] for i in range(sum(locationVector['users'][user][day].itervalues()))]
        completeDayDistribution+=dayDistributionForUser
        madOfDayDistributionForUsers.append(getMAD(dayDistributionForUser) )
    return getMAD(madOfDayDistributionForUsers), getMAD(completeDayDistribution) 

class MRLocationUserDayMad(ModifiedMRJob):
    DEFAULT_INPUT_PROTOCOL='raw_value'
    def mapper(self, key, locationVector):
        locationVector = cjson.decode(locationVector)
        usersMad, locationMad = getLocationUserSpecificMads(locationVector)
        yield locationVector['location'],  {'location': locationVector['location'], 'no_users': len(locationVector['users']),'users_db_mad': usersMad, 'location_db_mad': locationMad}

if __name__ == '__main__':
    MRLocationUserDayMad.run()
#    for l in FileIO.iterateJsonFromFile('../data/locationToUserAndTimeMap'):
#        print l
#        print getLocationUserSpecificMads(l)
#        exit()