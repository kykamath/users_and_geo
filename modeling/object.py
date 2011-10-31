'''
Created on Oct 27, 2011

@author: kykamath
'''
import numpy as np
import matplotlib.pyplot as plt
import random
from library.classes import GeneralMethods
from settings import conf
#from modeling.settings import noOfClustersPerArea, clusterSizeMean,\
#    clusterSizeStd

class ObjectTypes:
    USER = 'user'
    LOCATION = 'location'
    @staticmethod
    def cast(objectDict, type):
        if type == ObjectTypes.LOCATION: return Location.getObjectFromDict(objectDict)
    
class User:
    def __init__(self, area, userId, cluster):
        self.id = '%s_%s'%(area.id, userId)
        self.cluster = cluster
        self.locations = cluster.locations

class Demography:
    def __init__(self, area, demographyId):
        self.id = '%s_%s'%(area.id, demographyId)
        self.locations = []
    @staticmethod
    def getDemographyWithRandomLocations(area, demographyId, demographySizeMean, demographySizeStd):
        demography = Demography(area, demographyId)
        demography.locations = random.sample(area.getAllLocations(), int(random.gauss(demographySizeMean, demographySizeStd)))
        return demography

class Location:
    def __init__(self, lat=None, lon=None, area=None):
        if area:
            self.id = '%s_%0.3f_%0.3f'%(area.id, lat, lon)
            self.lat, self.lon = lat, lon
    #        self.totalClicks = None
            self.visitingProbability = GeneralMethods.getRandomNumberFromSimplePowerlawDistribution()
    @staticmethod
    def getLocationClassBasedOnVisitingProbability(location):
        for k, v in conf['locationClassesBasedOnVisitingProbability'].iteritems():
            if location.visitingProbability >= v[0] and location.visitingProbability <= v[1]: return k
    @staticmethod
    def getObjectFromDict(objectDict): 
        location = Location()
        location.__dict__ = objectDict
        return location

class Area:
    def __init__(self, areaLat, areaLon):
        self.id = '%0.3f_%0.3f'%(areaLat, areaLon)
        self.areaLat, self.areaLon = areaLat, areaLon
        self.locations = {'high': [], 'medium': [], 'low': []}
        self.users = []
        self.demographies = []
    def plot(self):
        plt.scatter([l.lat for l in self.locations], [l.lon for l in self.locations], marker = 'o', color='g')
#        plt.show()
    def getAllLocations(self): 
        locations = []
        for v in self.locations.itervalues(): locations+=v
        return locations
    @staticmethod
    def getArea(areaLat, areaLon, **conf):
        area = Area(areaLat, areaLon)
        mean = [areaLat, areaLon]; cov = [[conf['areaLatStd'],0], [0, conf['areaLonStd']]]
        for lat, lon in zip(*np.random.multivariate_normal(mean,cov,conf['noOfLocationsPerArea']).T): 
            location = Location(lat, lon, area)
            area.locations[Location.getLocationClassBasedOnVisitingProbability(location)].append(location)
        for demographyId in range(conf['noOfClustersPerArea']): area.demographies.append(Demography.getDemographyWithRandomLocations(area, demographyId, conf['demographySizeMean'], conf['demographySizeStd']))
        for userId in range(conf['noOfUsersPerArea']): area.users.append(User(area, userId, random.choice(area.demographies)))
        return area

#class Advertiser:
#    def __init__(self):
#        self.budget = None
#        self.interestedLocations = []

if __name__ == '__main__':
#    area = Area.getArea(10, 10, 5, 3, 1000, 50)
#    area.plot()
    for i in range(10):
        print GeneralMethods.weightedChoice([0.7, 0.15,0.15])
