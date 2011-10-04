'''
Created on Oct 3, 2011

@author: kykamath
'''
from mrjob.job import MRJob
import dateutil.parser
import cjson

def parseData(line):
    data = line.strip().split('\t')
    if len(data)!=7: data.append(None) 
    if len(data)==7: return {'_id':id, 'u': int(data[0]), 'tw': int(data[1]), 'l': [float(data[2]), float(data[3])], 't': dateutil.parser.parse(data[4]), 'x': data[5], 'lid': data[6]}

class MRUserToLocationMap(MRJob):
    def mapper(self, key, line):
        data = parseData(line)
        if data: yield data['u'], data['lid']
    def reducer(self, word, occurrences):
        occurences = [str(s) for s in occurrences]
        yield word, '\t'.join(list(set(occurences)))

if __name__ == '__main__':
    MRUserToLocationMap.run()