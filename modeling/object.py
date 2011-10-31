'''
Created on Oct 27, 2011

@author: kykamath
'''
import numpy as np
import matplotlib.pyplot as plt
import random
from library.classes import GeneralMethods
#from modeling.settings import noOfClustersPerArea, clusterSizeMean,\
#    clusterSizeStd

class User:
    def __init__(self, area, userId, cluster):
        self.id = '%s_%s'%(area.id, userId)
        self.cluster = cluster
        self.locations = cluster.locations

class UserCluster:
    def __init__(self, area, clusterId):
        self.id = '%s_%s'%(area.id, clusterId)
        self.locations = []
    @staticmethod
    def getClusterWithRandomLocations(area, clusterId, clusterSizeMean, clusterSizeStd):
        cluster = UserCluster(area, clusterId)
        cluster.locations = random.sample(area.locations, int(random.gauss(clusterSizeMean, clusterSizeStd)))
        return cluster

class Location:
    def __init__(self, lat, lon, area):
        self.id = '%s_%0.3f_%0.3f'%(area.id, lat, lon)
        self.lat, self.lon = lat, lon
#        self.totalClicks = None
        self.visitingProbability = random.random()

class Area:
    def __init__(self, areaLat, areaLon):
        self.id = '%0.3f_%0.3f'%(areaLat, areaLon)
        self.areaLat, self.areaLon = areaLat, areaLon
        self.locations = []
        self.users = []
        self.clusters = []
    def plot(self):
        plt.scatter([l.lat for l in self.locations], [l.lon for l in self.locations], marker = 'o', color='g')
#        plt.show()
    @staticmethod
    def getArea(areaLat, areaLon, **conf):
        area = Area(areaLat, areaLon)
        mean = [areaLat, areaLon]; cov = [[conf['areaLatStd'],0], [0, conf['areaLonStd']]]
        for lat, lon in zip(*np.random.multivariate_normal(mean,cov,conf['noOfLocationsPerArea']).T): area.locations.append(Location(lat, lon, area))
        for clusterId in range(conf['noOfClustersPerArea']): area.clusters.append(UserCluster.getClusterWithRandomLocations(area, clusterId, conf['clusterSizeMean'], conf['clusterSizeStd']))
        for userId in range(conf['noOfUsersPerArea']): area.users.append(User(area, userId, random.choice(area.clusters)))
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
