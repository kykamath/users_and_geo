'''
Created on Oct 30, 2011

@author: kykamath
'''
from settings import conf
from modeling.object import Area
import random
import matplotlib.pyplot as plt
from collections import defaultdict
from library.classes import GeneralMethods

class Model:
    def __init__(self, noOfAreas, **conf):
        self.areas = []
        self.conf = conf
        self.locationsCheckinsMap = defaultdict(dict)
        for area in range(noOfAreas):
            area = Area.getArea(random.randrange(*conf['areaLatitudeRange']), random.randrange(*conf['areaLongitudeRange']), **conf)
            self.areas.append(area)
#            area.plot()
#        plt.show()
    def run(self):
        for day in range(conf['noOfDaysOfSimulation']):
            for bin in range(conf['noOfBinsPerDay']):
                for area in self.areas: self.process((day, bin), area)
                print 'x'

    def process(self, (day, bin), area):
        for user in area.users:
            selectedLocation=GeneralMethods.weightedChoice([l.visitingProbability for l in user.locations])
            locationId = user.locations[selectedLocation].id
            if day not in self.locationsCheckinsMap[locationId]: self.locationsCheckinsMap[locationId][day] = {}
            if bin not in self.locationsCheckinsMap[locationId][day]: self.locationsCheckinsMap[locationId][day][bin] = []
            self.locationsCheckinsMap[locationId][day][bin].append(user.id)
        
            
if __name__ == '__main__':
    Model(1, **conf).run()