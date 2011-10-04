'''
Created on Oct 3, 2011

@author: kykamath
'''
from mrjob.job import MRJob

class MRUserToLocationMap(MRJob):
    def get_words(self, key, line):
        data = line.strip().split('\t')
        if len(data) == 7: data.append(None) 
        if len(data) == 7: yield {'_id':id, 'u': int(data[0]), 'tw': int(data[1]), 'l': [float(data[2]), float(data[3])], 't': dateutil.parser.parse(data[4]), 'x': data[5], 'lid': data[6]}

    def sum_words(self, word, occurrences):
        yield word, sum(occurrences)

    def steps(self):
        return [self.mr(self.get_words, self.sum_words),]

if __name__ == '__main__':
    MRUserToLocationMap.run()