'''
Created on Oct 7, 2011

@author: kykamath
'''
from analysis.mr_analysis import userToLocationMapIterator

def locationTransactionsIterator():
    def decrementDictionary(d):
        for k in d.keys()[:]:
            d[k]-=1
            if d[k]==0: del d[k]
    
    for d in userToLocationMapIterator():
        while d.keys(): 
            yield d.keys()
            decrementDictionary(d)

for t in locationTransactionsIterator():
    print t
    
