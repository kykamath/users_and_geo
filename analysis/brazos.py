'''
Created on Oct 21, 2011

@author: kykamath
'''
from analysis.mr_analysis import locationIterator
from library.geo import isWithinBoundingBox, getLocationFromLid
from settings import brazos_valley_boundary

i=0
for location in locationIterator():
    if isWithinBoundingBox(getLocationFromLid(location, brazos_valley_boundary)): print i, location; i+=1