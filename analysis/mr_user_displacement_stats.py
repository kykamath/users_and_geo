'''
Created on Oct 3, 2011

@author: kykamath
'''
from library.mrjobwrapper import ModifiedMRJob
from library.geo import parseData, getCenterOfMass, getRadiusOfGyration

class MRUserDisplacementStats(ModifiedMRJob):
    DEFAULT_INPUT_PROTOCOL='raw_value'
    def mapper(self, key, line):
        data = parseData(line)
        if data: yield data['u'], data['l'] 
    def reducer(self, user, occurrences): 
        points = list(occurrences)
        yield user, {'u': user, 'c': len(points), 'cm': list(getCenterOfMass(points)), 'rog': getRadiusOfGyration(points)}

if __name__ == '__main__':
    MRUserDisplacementStats.run()
