'''
Created on Oct 3, 2011

@author: kykamath
'''
import datetime
from library.mrjobwrapper import ModifiedMRJob

def parseData(line):
    data = line.strip().split('\t')
    if len(data)!=7: data.append(None) 
    if len(data)==7: return {'_id':id, 'u': int(data[0]), 'tw': int(data[1]), 'l': [float(data[2]), float(data[3])], 't': datetime.datetime.strptime(data[4], '%Y-%m-%d %H:%M:%S'), 'x': data[5], 'lid': data[6]}

class MRUserDistribution(ModifiedMRJob):
    DEFAULT_INPUT_PROTOCOL='raw_value'
    def mapper(self, key, line):
        data = parseData(line)
        if data: yield data['u'], 1
    def reducer(self, user, occurrences): yield user, {'user': user, 'count':sum(occurrences)}

if __name__ == '__main__':
    MRUserDistribution.run()
