from library.classes import GeneralMethods
from library.geo import geographicConvexHull, getLidFromLocation
from library.geo import getLocationFromLid
from library.file_io import FileIO

from collections import defaultdict
from operator import itemgetter

class SpotsKML:
    def __init__(self):
        import simplekml
        self.kml = simplekml.Kml()
    def addLidsWithHull(self, points, color=None, withAllPoints=False): self.addLocationPointsWithHull([getLocationFromLid(point) for point in points], color, withAllPoints)
    def addLocationPointsWithHull(self, points, color=None, withAllPoints=False):
        if not color: color=GeneralMethods.getRandomColor()
        points = [list(reversed(point)) for point in points]
        if not withAllPoints: self.kml.newpoint(coords=[points[0]])
        else: [self.kml.newpoint(coords=[point]) for point in points]
        pol=self.kml.newpolygon(outerboundaryis=geographicConvexHull(points))
        pol.polystyle.color = '99'+color[1:]
        pol.polystyle.outline = 0
    def addLocationPoints(self, points, color=None): 
        if not color: color=GeneralMethods.getRandomColor()
        for point in (list(reversed(point)) for point in points):
            pnt = self.kml.newpoint(coords=[point])
            pnt.iconstyle.icon.href = 'http://maps.google.com/mapfiles/kml/shapes/shaded_dot.png'
            pnt.iconstyle.color = 'ff'+color[1:]
    def addLocationPointsWithTitles(self, points, color=None): 
        if not color: color=GeneralMethods.getRandomColor()
        for point, title in ((list(reversed(point)), title) for point, title in points):
            pnt = self.kml.newpoint(description=title, coords=[point])
            pnt.iconstyle.icon.href = 'http://maps.google.com/mapfiles/kml/shapes/shaded_dot.png'
            pnt.iconstyle.color = 'ff'+color[1:]
    def addLine(self, points, description=None):
        from simplekml import LineStyle
        points = [list(reversed(getLocationFromLid(point))) for point in points]
        ls = self.kml.newlinestring(coords=points)
        self.kml.newpoint(coords=[points[0]]), self.kml.newpoint(coords=[points[-1]])
        if description: ls.description = description
        ls.linestyle = LineStyle(width=3.0)
    def write(self, fileName): self.kml.save(fileName)
    @staticmethod
    def drawKMLsForSpots(locationsIterator, outputKMLFile):
        kml = SpotsKML()
        for locations in locationsIterator: kml.addLidsWithHull(locations)
        kml.write(outputKMLFile)
    @staticmethod
    def drawKMLsForPointsAsHulls(locationsIterator, outputKMLFile):
        kml = SpotsKML()
        for locations in locationsIterator: kml.addLocationPointsWithHull(locations, withAllPoints=True)
        kml.write(outputKMLFile)
    @staticmethod
    def drawKMLsForSpotsWithPoints(locationsIterator, outputKMLFile, title=False):
        kml = SpotsKML()
        if not title:
            for l in  list(locationsIterator): kml.addLocationPoints(l)
        else: 
            for l in  list(locationsIterator): kml.addLocationPointsWithTitles(l)
        kml.write(outputKMLFile)
    @staticmethod
    def drawKMLsForPoints(pointsIterator, outputKMLFile, color=None):
        kml = SpotsKML()
        if not color: color = GeneralMethods.getRandomColor()
        kml.addLocationPoints(pointsIterator, color=color)
        kml.write(outputKMLFile)

class SpotsFile():
    @staticmethod
    def writeSpotsToFile(spotsIterator, spotsFile):
        i=0
        for spot in spotsIterator: FileIO.writeToFileAsJson({'id': i, 'spot': spot}, spotsFile); i+=1
    @staticmethod
    def writeUserDistributionInSpots(spotsFile, userToLocationVector):
        lidToSpotIdMap, userDistributionInSpots, spotsWithUsersFile = {}, defaultdict(list), spotsFile+'_users'
        for spot in FileIO.iterateJsonFromFile(spotsFile):
            for location, _ in spot['spot']: 
                lidToSpotIdMap[getLidFromLocation(location)] = spot['id']
            userDistributionInSpots[spot['id']] = {'id': spot['id'], 'lids': spot['spot'], 'users': defaultdict(list)}
        for userObject in userToLocationVector: 
            userId, userVector = userObject['user'], userObject['locations']
            spotDistribution = defaultdict(int)
            for lid in userVector: 
                if lid in lidToSpotIdMap: spotDistribution[lidToSpotIdMap[lid]]+=1
            if spotDistribution: 
                spotId = sorted(spotDistribution.iteritems(), key=itemgetter(1))[-1][0]
                userDistributionInSpots[spotId]['users'].append(userId)
        for spotId, objects in userDistributionInSpots.iteritems(): print spotId, objects
            