'''
Created on Oct 31, 2011

@author: kykamath
'''
from models import Model
from modeling.settings import conf
from collections import defaultdict
from library.file_io import FileIO
import matplotlib.pyplot as plt
from modeling.object import Location

def drawAllCheckinPlotsByVisitingClasses(model, **conf):
    plotsFolder = conf['plotsFolder']+'byVisitingClasses/'
    for locationId, location in model.locationsCheckinsMap.iteritems():
        if location['checkins']: 
            locationObject = Location.getObjectFromDict(location['object'])
            plotsFile = '%s%s/%s'%(plotsFolder, Location.getLocationClassBasedOnVisitingProbability(locationObject),locationId+'.png')
            FileIO.createDirectoryForFile(plotsFile)
            checkinsByBins = defaultdict(int)
            for day, binData in location['checkins'].iteritems():
                for bin, checkins in binData.iteritems():
                    checkinsByBins[int(bin)]+=len(checkins)
            plt.plot(checkinsByBins.keys(), checkinsByBins.values())
            plt.savefig(plotsFile)
            plt.clf()
if __name__ == '__main__':
    model = Model(**conf)
    model.loadSimulationData()
    drawAllCheckinPlotsByVisitingClasses(model, **conf)