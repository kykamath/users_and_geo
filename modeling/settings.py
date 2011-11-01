'''
Created on Oct 30, 2011

@author: kykamath
'''
conf = dict(
            noOfBinsPerDay = 48,
            noOfDaysOfSimulation = 1,
            noOfAreas = 1,
            simulationDataFolder = './simulation_data/',
            plotsFolder = '/data/geo/model/plots/',
            
            areaLatitudeRange = [0,1000],
            areaLongitudeRange = [0,1000],
            areaLatStd = 10,
            areaLonStd = 10,
            noOfLocationsPerArea = 1000,
            noOfUsersPerArea = 10000,
            
            locationClassesBasedOnVisitingProbability = dict(high=[0.75, 1.0], medium=[0.25, 0.75], low=[0.0, 0.25]),
            
            noOfClustersPerArea = 5,
            demographySizeMean = 10,
            demographySizeStd = 2,
            )
