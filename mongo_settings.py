'''
Created on Oct 3, 2011

@author: kykamath
'''
from pymongo import Connection, GEO2D, ASCENDING

geoDb = Connection('sid.cs.tamu.edu').geo
#geoDb = Connection('localhost').geo

'''
{
'_id': tweet id,  
'u': user id, 
'l': [latitude, longitude], 
'lid': location id
't': checkin time, 
'x': text, 
'pid': place id
}
'''
checkinsCollection = geoDb.checkins
checkinsCollection.create_index([ ('l', GEO2D), ('u', ASCENDING), ('t', ASCENDING), ('lid', ASCENDING), ('pid', ASCENDING)])

'''
{
'_id': place id, 
'n': place name,
'l': [latitude, longitude], 
'lid': location id
'm': meta info, 
'tp': total people checked in, 
'tc': total checkins, 
}
'''
venuesCollection = geoDb.venues
venuesCollection.create_index([ ('l', GEO2D), ('lid', ASCENDING), ('tp', ASCENDING), ('tc', ASCENDING)])

#'''
#{
#'_id': user id, 
#'tc': total user checkins, 
#}
#'''
#usersCollection = geoDb.users
#usersCollection.create_index([ ('tc', ASCENDING)])

'''
{
'_id': location id (lid), 
'l': [latitude, longitude], 
'tc': total user checkins, 
}
'''
locationsCollection = geoDb.locations
locationsCollection.create_index([ ('l', GEO2D), ('tc', ASCENDING)])

'''
{
'_id': "lid1 lid2"
'u': # of common users (>=10)
'd': Haversine distance in miles (<=10)
}
'''
locationToLocationCollection = geoDb.location_to_location
locationsCollection.create_index('u')
locationsCollection.create_index('d')