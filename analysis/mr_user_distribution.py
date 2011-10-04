'''
Created on Oct 3, 2011

@author: kykamath
'''
import dateutil.parser
from mrjob.job import MRJob

def parseData(line):
    data = line.strip().split('\t')
    if len(data)!=7: data.append(None) 
    if len(data)==7: return {'_id':id, 'u': int(data[0]), 'tw': int(data[1]), 'l': [float(data[2]), float(data[3])], 't': dateutil.parser.parse(data[4]), 'x': data[5], 'lid': data[6]}

class MRUserDistribution(MRJob):
    def mapper(self, key, line):
        data = parseData(line)
        if data: yield data['u'], 1
    def reducer(self, word, occurrences): yield word, sum(occurrences)

if __name__ == '__main__':
    MRUserDistribution.run()