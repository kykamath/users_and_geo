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

def cluster(algorithm, filename, options = ''):
    reader = BufferedReader(FileReader(filename))
    data = Instances(reader)
    reader.close()
    cl = algorithm()
    cl.setOptions(options.split())
    cl.buildClusterer(data)
    returnData = []
    for instance in data.enumerateInstances(): returnData.append(cl.clusterInstance(instance))
    return returnData

print set(cluster(SimpleKMeans, '/Applications/Weka/weka-3-6-6/data/iris.arff', '-N 5'))
