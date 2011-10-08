'''
Created on Oct 7, 2011

@author: kykamath
'''
import sys
sys.path.append('../')
from settings import expLocationRadiusInMiles, expMinimumLocationsPerSpot
from analysis.spots_radius import drawKMLsForRadiusSpots


def generateRadiusSpots(): drawKMLsForRadiusSpots(expLocationRadiusInMiles, expMinimumLocationsPerSpot)

if __name__ == '__main__':
    generateRadiusSpots()
    