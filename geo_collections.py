'''
Created on Oct 3, 2011

@author: kykamath
'''
from pymongo import Connection, GEO2D, ASCENDING

geoDb = Connection('sid.cs.tamu.edu').geo

'''
{
'_id':checkin id, 
'u': user id, 
'tw': tweet id, 
'l': [latitude, longitude], 
't': checkin time, 
'x': text, 
'lid': location id
}
'''
checkinsCollection = geoDb.checkins
checkinsCollection.create_index([ ('l', GEO2D), ('uid', ASCENDING), ('t', ASCENDING), ('lid', ASCENDING)])