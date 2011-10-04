'''
Created on Oct 3, 2011

@author: kykamath
'''
import dateutil.parser
from geo_collections import checkinsCollection

checkins_file = '/mnt/chevron/kykamath/data/geo/checkin_data.txt'

def iterateCheckins():
    id = 1
    for data in open(checkins_file): 
        data = data.strip().split('\t')
        if len(data)!=7: data.append(None) 
        yield {'_id':id, 'u': int(data[0]), 'tw': int(data[1]), 'l': [float(data[2]), float(data[3])], 't': dateutil.parser.parse(data[4]), 'x': data[5], 'lid': data[6]}
        id+=1
        if id==10: break
    
def addCheckinsToDB():
    for checkin in iterateCheckins(): checkinsCollection.insert(checkin)
        
if __name__ == '__main__':
    addCheckinsToDB()