'''
Created on Oct 3, 2011

@author: kykamath
'''
import sys
sys.path.append('../')
from bson.code import Code
from geo_collections import geoDb, checkinsCollection
from collections import defaultdict
import matplotlib.pyplot as plt

def generateLocationDistribution():
    map = Code('function () { emit(this.lid, 1); }')
    reduce = Code("function (key, values) {"
                  "  var total = 0;"
                  "  for (var i = 0; i < values.length; i++) {"
                  "    total += values[i];"
                  "  }"
                  "  return total;"
                  "}")
    checkinsCollection.map_reduce(map, reduce, "locationDistribution")
    
def generateUserCheckinDistribution():
    map = Code('function () { emit(this.u, 1); }')
    reduce = Code("function (key, values) {"
                  "  var total = 0;"
                  "  for (var i = 0; i < values.length; i++) {"
                  "    total += values[i];"
                  "  }"
                  "  return total;"
                  "}")
    checkinsCollection.map_reduce(map, reduce, "userCheckinDistribution")
    
def locationDistributionIterator(): return (k for k in geoDb.locationDistribution.find())
def userCheckinDistributionIterator(): return (k for k in geoDb.userCheckinDistribution.find())
    
def plotDistribution(dataIterator, fileName='distribution'):
    dist = defaultdict(int)
    for k in dataIterator: dist[k['value']]+=1
    dataX, dataY = sorted(dist), [dist[x] for x in sorted(dist)]
    plt.loglog(dataX, dataY)
    print dataX
    print dataY
    plt.savefig('%s.pdf'%fileName), plt.savefig('%s.eps'%fileName)


if __name__ == '__main__':
#    kwargs = {'fileName':'locationDistribution'}
#    plotDistribution(locationDistributionIterator(), **kwargs)

    generateLocationDistribution()
#    generateUserCheckinDistribution()
