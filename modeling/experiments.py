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
from library.plotting import smooth

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
#            print len(checkinsByBins.keys()), len(smooth(checkinsByBins.values(), 1)[:48])
            plt.plot(checkinsByBins.keys(), smooth(checkinsByBins.values(), 1))
#            plt.hist([k for k, v in checkinsByBins.iteritems() for i in range(v)], conf['noOfBinsPerDay'], normed=True)
            plt.title(str(locationObject.visitingProbability))
            plt.savefig(plotsFile)
            plt.clf()
            
def drawAllCheckinPlotsByVisitingClassesUsingDemography(model, **conf):
    plotsFolder = conf['plotsFolder']+'byVisitingClassesUsingDemography/'
    for locationId, location in model.locationsCheckinsMap.iteritems():
        if location['checkins']: 
            locationObject = Location.getObjectFromDict(location['object'])
            plotsFile = '%s%s/%s'%(plotsFolder, Location.getLocationClassBasedOnVisitingProbability(locationObject),locationId+'.png')
            FileIO.createDirectoryForFile(plotsFile)
            checkinsByBinsAndDemographies = defaultdict(dict)
            demographColorMap = {}
            for day, binData in location['checkins'].iteritems():
                for bin, checkins in binData.iteritems():
                    bin=int(bin)
                    for user in checkins:
                        demographyId = model.userMap[user]['object']['demography_id']
                        demographColorMap[demographyId] = model.userMap[user]['object']['demography_color']
                        if bin not in checkinsByBinsAndDemographies[demographyId]: checkinsByBinsAndDemographies[demographyId][bin]=0
                        checkinsByBinsAndDemographies[demographyId][bin]+=1
#            for bin in checkinsByBinsAndDemographies:
#                for demographyId in demographColorMap:
#                    plt.scatter([bin], [checkinsByBinsAndDemographies[bin][demographyId]], color=demographColorMap[demographyId])
            for demographyId, data in checkinsByBinsAndDemographies.iteritems():
#                print smooth([data[k] for k in sorted(data)], 4)
                plt.fill_between(sorted(data.keys()), smooth([data[k] for k in sorted(data)], 10)[:len(data)], color=demographColorMap[demographyId], alpha=0.65)
#               plt.hist([k for k, v in checkinsByBins.iteritems() for i in range(v)], conf['noOfBinsPerDay'], normed=True)
            plt.title(str(locationObject.visitingProbability))
            plt.savefig(plotsFile)
            plt.clf()
#            plt.show()
if __name__ == '__main__':
    model = Model(**conf)
    model.loadSimulationData()
#    drawAllCheckinPlotsByVisitingClasses(model, **conf)
    drawAllCheckinPlotsByVisitingClassesUsingDemography(model, **conf)
