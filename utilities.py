'''
Created on Oct 3, 2011

@author: kykamath
'''
import cjson
from library.file_io import FileIO

checkins_file = '/mnt/chevron/kykamath/data/geo/checkin_data.txt'
checkins_json_file = '/mnt/chevron/kykamath/data/geo/checkin_data.json'

def iterateCheckins():
    for l in open(checkins_file):
        data = l.split()
        FileIO.writeToFileAsJson(cjson.encode({'uid': data[0], 'twid': data[1], 'lat': data[2],'lon': data[3], 't': data[4], 'text': data[5], 'pid': data[6]}), checkins_json_file)
        
if __name__ == '__main__':
    iterateCheckins()