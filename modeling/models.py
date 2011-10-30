'''
Created on Oct 30, 2011

@author: kykamath
'''
from settings import conf
from modeling.object import Area
import random
import matplotlib.pyplot as plt

class Model:
    def __init__(self, noOfAreas, **conf):
        self.areas = []
        self.conf = conf
        for area in range(noOfAreas):
            area = Area.getArea(random.randrange(*conf['areaLatitudeRange']), random.randrange(*conf['areaLongitudeRange']), **conf)
            self.areas.append(area)
#            area.plot()
#        plt.show()
    def run(self):
        for day in range(conf['noOfDaysOfSimulation']):
            for bin in range(conf['noOfBinsPerDay']):
                for area in self.areas: self.process((day, bin), area)
    def process(self, timeTuple, area):
        for user in area.users:
            print user
                    
        
            
if __name__ == '__main__':
    Model(1, **conf).run()