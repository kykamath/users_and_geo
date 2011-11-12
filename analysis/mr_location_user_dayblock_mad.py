'''
Created on Oct 12, 2011

@author: kykamath
'''
from library.mrjobwrapper import ModifiedMRJob
from library.math_modified import getMAD
import cjson
from library.file_io import FileIO
from library.plotting import getDataDistribution

def getLocationUserSpecificMads(locationVector):
    completeDayBlockDistribution, madOfDayBlockDistributionForUsers = [], []
    for user in locationVector['users']:
        dayBlockDistributionForUser = []
        for day in locationVector['users'][user]:
            dayBlockDistributionForUser+=[int(dayBlock) for dayBlock in locationVector['users'][user][day] for i in range(locationVector['users'][user][day][dayBlock])]
        completeDayBlockDistribution+=dayBlockDistributionForUser
        madOfDayBlockDistributionForUsers.append(getMAD(dayBlockDistributionForUser) )
    dataX, dataY = getDataDistribution(completeDayBlockDistribution)
    return getMAD(madOfDayBlockDistributionForUsers), getMAD(completeDayBlockDistribution) , dict((str(x),y) for x,y in zip(dataX, dataY))

class MRLocationUserDayBlockMad(ModifiedMRJob):
    DEFAULT_INPUT_PROTOCOL='raw_value'
    def mapper(self, key, locationVector):
        locationVector = cjson.decode(locationVector)
        usersMad, locationMad, dbDistribution = getLocationUserSpecificMads(locationVector)
        yield locationVector['location'],  {'location': locationVector['location'], 'no_users': len(locationVector['users']),'users_db_mad': usersMad, 'location_db_mad': locationMad, 'db_dis': dbDistribution}

if __name__ == '__main__':
    MRLocationUserDayBlockMad.run()
#    for line in FileIO.iterateJsonFromFile('../data/locationToUserAndTimeMap'):
#        print getLocationUserSpecificMads(line)