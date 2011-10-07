'''
Created on Oct 7, 2011

@author: kykamath
'''
from analysis.mr_analysis import userToLocationMapIterator
from library.file_io import FileIO
from settings import locationsFIMahoutInputFile

def locationTransactionsIterator():
    i = 0
    def decrementDictionary(d):
        for k in d.keys()[:]:
            d[k]-=1
            if d[k]==0: del d[k]
    
    for d in userToLocationMapIterator():
        while d.keys(): 
            yield d.keys()
            decrementDictionary(d)
        i+=1
        if i==10: break

def generateFileForFIMahout(): 
    [FileIO.writeToFile(' '.join([i.replace(' ', '_') for i in t]), locationsFIMahoutInputFile) for t in locationTransactionsIterator()]
