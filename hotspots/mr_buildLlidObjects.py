'''
Created on Nov 16, 2011

@author: kykamath
'''
from library.mrjobwrapper import ModifiedMRJob
import cjson

class MRBuildLlidObjects(ModifiedMRJob):
    DEFAULT_INPUT_PROTOCOL='raw_value'
    def mapper(self, key, line):
        data = cjson.decode(line)
        yield data['llid'], {'u':data['u'], 'l': data['l'], 't': data['t']}
    def reducer(self, llid, checkins): yield llid, {'llid':llid, 'checkins': list(checkins)}

if __name__ == '__main__':
    MRBuildLlidObjects.run()