'''
Created on Oct 7, 2011

@author: kykamath
'''
import os
from analysis.mr_analysis import userToLocationMapIterator
from library.file_io import FileIO
from settings import locationsFIMahoutInputFile, locationsFIMahoutOutputFile,\
    minimumTransactionLength, minSupport, userBasedSpotsKmlsFolder
from analysis import SpotsKML

userBasedSpotsUsingFIKmlsFolder=userBasedSpotsKmlsFolder+'fi/'

def locationTransactionsIterator():
    i = 0
    def decrementDictionary(d):
        for k in d.keys()[:]:
            d[k]-=1
            if d[k]==0: del d[k]
    
    for d in userToLocationMapIterator():
        while len(d.keys())>=minimumTransactionLength: 
            yield d.keys()
            decrementDictionary(d)
        i+=1
        print i
#        if i==10: break

def writeInputFileForFIMahout(): [FileIO.writeToFile(' '.join([i.replace(' ', '_') for i in t]), locationsFIMahoutInputFile) for t in locationTransactionsIterator()]

def calculateFrequentLocationItemsets():
    os.system('tar -cvf %s.tar %s'%(locationsFIMahoutInputFile, locationsFIMahoutInputFile))
    os.system('gzip %s.tar'%(locationsFIMahoutInputFile))
    os.system('hadoop fs -put %s.tar.gz fi/.'%locationsFIMahoutInputFile)
    os.system('mahout fpg -i fi/mh_input.tar.gz -o fi/output -k 50 -method mapreduce -s %s'%minSupport)
def getMahoutOutput(): os.system('mahout seqdumper -s fi/output/frequentpatterns/part-r-00000 > %s'%locationsFIMahoutOutputFile)
    
def iterateFrequentLocationsFromFIMahout(minSupport=minSupport, minLocations=6): 
    for line in FileIO.iterateLinesFromFile(locationsFIMahoutOutputFile):
        if line.startswith('Key:'): 
            data = line.split('Value: ')[1][1:-1].split(',')
            locationItemset = [i.replace('_', ' ') for i in data[0][1:-1].split()]
            if int(data[1])>minSupport and len(locationItemset)>=minLocations: yield locationItemset 

def drawKMLsForUserBasedSpotsUsingFI(minSupport=minSupport, minLocations=6):
    SpotsKML.drawKMLsForSpots(iterateFrequentLocationsFromFIMahout(minSupport, minLocations), userBasedSpotsUsingFIKmlsFolder+'%s_%s.kml'%(minSupport, minLocations))
    
    
if __name__ == '__main__':
#    writeInputFileForFIMahout()
#    calculateFrequentLocationItemsets()
    getMahoutOutput()
#    drawKMLsForUserBasedSpotsUsingFI()