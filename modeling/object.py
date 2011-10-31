'''
Created on Oct 27, 2011

@author: kykamath
'''
import numpy as np
import matplotlib.pyplot as plt
import random
from library.classes import GeneralMethods
from settings import conf
from itertools import combinations
from scipy.stats import norm 
#from modeling.settings import noOfClustersPerArea, clusterSizeMean,\
#    clusterSizeStd

class ObjectTypes:
    USER = 'user'
    LOCATION = 'location'
    @staticmethod
    def cast(objectDict, type):
        if type == ObjectTypes.LOCATION: return Location.getObjectFromDict(objectDict)
    
class User:
    def __init__(self, area, userId, demography):
        self.id = '%s_%s'%(area.id, userId)
        self.demography = demography
        self.locations = demography.locations
        self.checkinginProbability = demography.userCheckinginProbability

class Demography:
    def __init__(self, area, demographyId):
        self.id = '%s_%s'%(area.id, demographyId)
        self.locations = []
        self.userCheckinginProbability = random.uniform(0.5,1.0)
    @staticmethod
    def getDemographyWithRandomLocations(area, demographyId, demographySizeMean, demographySizeStd):
        demography = Demography(area, demographyId)
        demography.locations = random.sample(area.getAllLocations(), int(random.gauss(demographySizeMean, demographySizeStd)))
        return demography

class LocationBin:
    def __init__(self, noOfBins):
        self.noOfBins = noOfBins
        self.probabilities = []
#        for i in range(self.noOfBins):
#            l = [0]*self.noOfBins
#            l[i] = 1
#            self.probabilities.append([i, LocationBin.assignProbabilities(l)])
        for i in range(noOfBins):
            probability = []
            for j in range(noOfBins):
                probability.append(random.uniform(0.0, 0.10))
                if j>=i-2 and j<=i+2: probability[j] = norm.pdf(j, loc=i, scale=2)*4
            self.probabilities.append([i, probability])
        self.probabilities.append([noOfBins, [random.uniform(0.6, 1.0) for i in range(noOfBins)]])
    @staticmethod
    def assignProbabilities(bins):
        assignedProbabilities = []
        for b in bins:
            if b==1: assignedProbabilities.append(random.uniform(0.75,1))
            else: assignedProbabilities.append(random.uniform(0.0, 0.25))
        return assignedProbabilities

class Location:
    def __init__(self, lat=None, lon=None, area=None, locationBin=None):
        if area:
            self.id = '%s_%0.3f_%0.3f'%(area.id, lat, lon)
            self.lat, self.lon = lat, lon
    #        self.totalClicks = None
            self.visitingProbability = GeneralMethods.getRandomNumberFromSimplePowerlawDistribution()
            self.binProbability = random.choice(locationBin.probabilities)
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
        locationBin = LocationBin(conf['noOfBinsPerDay'])
        for lat, lon in zip(*np.random.multivariate_normal(mean,cov,conf['noOfLocationsPerArea']).T): 
            location = Location(lat, lon, area, locationBin)
            area.locations[Location.getLocationClassBasedOnVisitingProbability(location)].append(location)
        for demographyId in range(conf['noOfClustersPerArea']): area.demographies.append(Demography.getDemographyWithRandomLocations(area, demographyId, conf['demographySizeMean'], conf['demographySizeStd']))
        for userId in range(conf['noOfUsersPerArea']): area.users.append(User(area, userId, random.choice(area.demographies)))
        return area

#class Advertiser:
#    def __init__(self):
#        self.budget = None
#        self.interestedLocations = []

if __name__ == '__main__':
#    def gaussianDistribution(mu, sigma, x):
#        print (1/np.sqrt(2*np.pi*(sigma**2)))*np.exp(-1*(((x-mu)**2)/(2*(sigma**2))))
##    l = LocationBin(3)
##    print l.probabilities
#    gaussianDistribution(13, 1, 13)

#    for i in range(24):
#        print i, norm.pdf(i, loc=13, scale=2)*4
    noOfBins = 3
    for i in range(noOfBins):
        probability = []
        for j in range(noOfBins):
            probability.append(random.uniform(0.0, 0.25))
            if j>=i-2 and j<=i+2: probability[j] = norm.pdf(j, loc=i, scale=2)*4
        print probability
