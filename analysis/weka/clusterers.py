'''
Created on Nov 8, 2011

@author: kykamath
'''
import os
from weka.core import Instances
from optparse import OptionParser
from java.io import BufferedReader, FileReader
from weka.clusterers import ClusterEvaluation
from weka.clusterers import EM, SimpleKMeans
from weka.core import Instances
from weka import ARFF

def cluster(algorithm, data, options = ''):
    filename = ARFF.writeARFFForClustering(data, 'data')
    print filename
    exit()
    reader = BufferedReader(FileReader(filename))
    data = Instances(reader)
    reader.close()
    cl = algorithm()
    cl.setOptions(options.split())
    cl.buildClusterer(data)
    returnData = []
    for instance in data.enumerateInstances(): returnData.append(cl.clusterInstance(instance))
    return returnData

data = {1: {'a':10, 'b': 15},
        2: {'c':10},
        3: {'a':10, 'b': 15}}

#print set(cluster(SimpleKMeans, '/Applications/Weka/weka-3-6-6/data/iris.arff', '-N 5'))
print set(cluster(SimpleKMeans, data, '-N 5'))

