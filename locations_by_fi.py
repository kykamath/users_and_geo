'''
Created on Oct 7, 2011

@author: kykamath
'''
from analysis.mr_analysis import userToLocationMapIterator
i = 0
for d in userToLocationMapIterator():
    print d
    i+=1
    if i==5: break