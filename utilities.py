'''
Created on Oct 3, 2011

@author: kykamath
'''
import dateutil.parser
from geo_collections import checkinsCollection

checkins_file = '/mnt/chevron/kykamath/data/geo/checkin_data.txt'
checkin_data_reduced_file = '/mnt/chevron/kykamath/data/geo/checkin_data_reduced.txt'

def iterateCheckins():
    id = checkinsCollection.count()+1
    for data in open(checkins_file): 
#    for data in open(checkin_data_reduced_file): 
        data = data.strip().split('\t')
        if len(data)!=7: data.append(None) 
        if len(data) == 7: yield {'_id':id, 'u': int(data[0]), 'tw': int(data[1]), 'l': [float(data[2]), float(data[3])], 't': dateutil.parser.parse(data[4]), 'x': data[5], 'lid': data[6]}
        else: print 'Problem in line: ', id, data
        id+=1
#        if id==1000: break;
    
def addCheckinsToDB():
    for checkin in iterateCheckins(): checkinsCollection.insert(checkin)
        
if __name__ == '__main__':
    addCheckinsToDB()
