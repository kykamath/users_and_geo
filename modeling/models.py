'''
Created on Oct 30, 2011

@author: kykamath
'''
from settings import conf
from modeling.object import Area, ObjectTypes
import random
import matplotlib.pyplot as plt
from collections import defaultdict
from library.classes import GeneralMethods
from library.plotting import getDataDistribution
from library.file_io import FileIO
from scipy.stats import bernoulli

class Model:
    def __init__(self, **conf):
        self.areas = []
        self.conf = conf
        self.locationsCheckinsMap = {}
        self.userMap = {}
        self.modelType = 'basic'
        self.simulationFile = self.getSimulationFile()
    def run(self):
        self.initializeAreas()
        for day in range(conf['noOfDaysOfSimulation']):
            for bin in range(conf['noOfBinsPerDay']):
                for area in self.areas: 
                    print 'Processing: day=%s, bin=%s, area=%s'%(day, bin, area.id)
                    self.process((day, bin), area)
        self.writeSimulationData()
    def initializeAreas(self):
        for area in range(self.conf['noOfAreas']):
            area = Area.getArea(random.randrange(*self.conf['areaLatitudeRange']), random.randrange(*self.conf['areaLongitudeRange']), **self.conf)
            for location in area.getAllLocations(): self.locationsCheckinsMap[location.id] = {'checkins': defaultdict(dict), 'object': location.__dict__, 'type': ObjectTypes.LOCATION} 
            self.areas.append(area)
        for area in self.areas:
            for user in area.users: self.userMap[user.id] = {'object': {'id': user.id, 'demography_id': user.demography.id, 'demography_color': user.demography.color}, 'type': ObjectTypes.USER}
    def process(self, (day, bin), area):
        day, bin = str(day), str(bin)
        for user in area.users:
            if bernoulli.rvs(user.checkinginProbability):
                selectedLocation=GeneralMethods.weightedChoice([l.visitingProbability*l.binProbability[1][int(bin)]*user.locationVisitingProbability[l.id][int(bin)] for l in user.locations])
                locationId = user.locations[selectedLocation].id
                if day not in self.locationsCheckinsMap[locationId]['checkins']: self.locationsCheckinsMap[locationId]['checkins'][day] = {}
                if bin not in self.locationsCheckinsMap[locationId]['checkins'][day]: self.locationsCheckinsMap[locationId]['checkins'][day][bin] = []
                self.locationsCheckinsMap[locationId]['checkins'][day][bin].append(user.id)
    def getSimulationFile(self): 
        file = self.conf['simulationDataFolder']+'%s/%s_%s_%s'%(self.modelType, self.conf['noOfDaysOfSimulation'], self.conf['noOfBinsPerDay'], self.conf['noOfAreas'])
        FileIO.createDirectoryForFile(file)
        return file
    def writeSimulationData(self):
        GeneralMethods.runCommand('rm -rf %s'%self.simulationFile)
        for location, data in self.locationsCheckinsMap.iteritems(): FileIO.writeToFileAsJson(data, self.simulationFile)
        for user, data in self.userMap.iteritems(): FileIO.writeToFileAsJson(data, self.simulationFile)
#        for area in self.areas:
#            for user in area.users: FileIO.writeToFileAsJson({'object': {'id': user.id, 'demography_id': user.demography.id}, 'type': ObjectTypes.USER}, self.simulationFile)
    def loadSimulationData(self):
        for data in FileIO.iterateJsonFromFile(self.simulationFile):
            if data['type']==ObjectTypes.LOCATION: self.locationsCheckinsMap[data['object']['id']]=data
            if data['type']==ObjectTypes.USER: self.userMap[data['object']['id']]=data
                
if __name__ == '__main__':
    Model(**conf).run()
