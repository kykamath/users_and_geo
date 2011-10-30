'''
Created on Oct 27, 2011

@author: kykamath
'''
class User:
    def __init__(self):
        self.locations = []

class Location:
    def __init__(self):
        self.totalClicks = None

class Advertiser:
    def __init__(self):
        self.budget = None
        self.interestedLocations = []